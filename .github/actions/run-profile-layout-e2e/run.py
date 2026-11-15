import base64
import datetime
import errno
import hashlib
import hmac
import http.server
import json
import os
import pathlib
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

API = "https://localhost:8017/api2/extjs/config/"
TLS = ssl._create_unverified_context()
ACCESS = "layout-reader"
SECRET = "layout-reader-test-secret"
NFS_PORT = 32159
S3_PORT = 39159

def command(*args, env=None):
	result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=180)
	if result.returncode:
		raise RuntimeError(f"{args[0]} failed: {result.stdout}\n{result.stderr}")
	return result.stdout

def api(path, method="GET", fields=None, status=200):
	data = urllib.parse.urlencode(fields).encode() if fields is not None else None
	request = urllib.request.Request(API + path, data=data, method=method)
	try:
		response = urllib.request.urlopen(request, context=TLS, timeout=150)
	except urllib.error.HTTPError as error:
		response = error
	with response:
		body = response.read()
		assert response.status == status, (path, response.status, body)
		decoded = json.loads(body)
		if status == 200:
			assert decoded.get("success", True), decoded
		return decoded.get("data")

def wait(description, predicate, timeout=120):
	deadline = time.monotonic() + timeout
	while time.monotonic() < deadline:
		if predicate():
			print("PASS:", description, flush=True)
			return
		time.sleep(1)
	raise TimeoutError(description)

class Resolver(http.server.BaseHTTPRequestHandler):
	mode = "normal"
	requests = []

	def do_POST(self):
		metadata = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
		type(self).requests.append(metadata)
		if self.mode == "error":
			self.send_error(503)
			return
		values = ["Project A", "Project B", "Project A"] if metadata["namespace"].endswith("D-100") else []
		body = json.dumps({"values": values}).encode()
		self.send_response(200)
		self.send_header("Content-Type", "application/json")
		self.send_header("Content-Length", str(len(body)))
		self.end_headers()
		self.wfile.write(body)

	def log_message(self, *args):
		pass

def s3(method, key="", query=None, body=b"", status=200):
	path = "/layout-data/" + urllib.parse.quote(key, safe="/-_.~")
	query_string = urllib.parse.urlencode(sorted((query or {}).items()), quote_via=urllib.parse.quote)
	now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
	host = f"127.0.0.1:{S3_PORT}"
	digest = hashlib.sha256(body).hexdigest()
	headers = {"host": host, "x-amz-content-sha256": digest, "x-amz-date": now}
	signed_headers = ";".join(sorted(headers))
	canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
	canonical = "\n".join([method, path, query_string, canonical_headers, signed_headers, digest])
	scope = now[:8] + "/us-east-1/s3/aws4_request"
	to_sign = "AWS4-HMAC-SHA256\n" + now + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
	key_bytes = ("AWS4" + SECRET).encode()
	for component in [now[:8], "us-east-1", "s3", "aws4_request"]:
		key_bytes = hmac.new(key_bytes, component.encode(), hashlib.sha256).digest()
	signature = hmac.new(key_bytes, to_sign.encode(), hashlib.sha256).hexdigest()
	headers["Authorization"] = f"AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
	url = f"http://{host}{path}" + ("?" + query_string if query_string else "")
	request = urllib.request.Request(url, data=body if method == "PUT" else None, headers=headers, method=method)
	try:
		response = urllib.request.urlopen(request, timeout=30)
	except urllib.error.HTTPError as error:
		response = error
	with response:
		result = response.read()
		assert response.status == status, (method, key, response.status, result)
		return result

def main():
	with tempfile.TemporaryDirectory(prefix="profile-layout-e2e-", dir="/mnt") as temporary:
		work = pathlib.Path(temporary)
		work.chmod(0o755)
		name = "layout-e2e-" + str(os.getpid())
		ds_path = work / "datastore"
		token = None
		store_created = False
		profiles = []
		outposts = []
		server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Resolver)
		thread = threading.Thread(target=server.serve_forever, daemon=True)
		thread.start()
		encoded_ds = base64.urlsafe_b64encode(name.encode()).decode()

		def sessions():
			return [s for s in api("d2d-mounts") if s["datastore"] == name]

		def detach_all():
			for session in sessions():
				api("d2d-unmount/" + encoded_ds, "POST", {"ns": session["namespace"], "backup-type": session["backup-type"], "backup-id": session["backup-id"], "backup-time": session["backup-time"], "file-name": session["file-name"], "force": "1"})
			wait("snapshot sessions detached", lambda: not sessions())

		try:
			command("proxmox-backup-manager", "datastore", "create", name, str(ds_path))
			store_created = True
			token = json.loads(command("proxmox-backup-manager", "user", "generate-token", "root@pam", name).removeprefix("Result: "))
			command("proxmox-backup-manager", "acl", "update", "/datastore/" + name, "DatastoreAdmin", "--auth-id", token["tokenid"])
			cert = pathlib.Path("/etc/proxmox-backup/proxy.pem").read_text()
			fingerprint = ":".join(f"{byte:02x}" for byte in hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert)).digest())
			env = dict(os.environ, PBS_PASSWORD=token["value"], PBS_FINGERPRINT=fingerprint, PBS_REPOSITORY=f"root@pam!{name}@localhost:{name}")
			for namespace in ["Media", "Media/D-100", "Media/D-200"]:
				command("proxmox-backup-client", "namespace", "create", namespace, env=env)
			base_time = int(time.time()) - 600

			def backup(namespace, host, offset, contents, detection="legacy"):
				source = work / "source"
				source.mkdir(exist_ok=True)
				(source / "marker.txt").write_text(contents)
				command("proxmox-backup-client", "backup", f"data.pxar:{source}", "--ns", namespace, "--backup-id", host, "--backup-time", str(base_time + offset), "--change-detection-mode", detection, env=env)

			backup("Media/D-100", "scanner-old", 0, "older snapshot")
			backup("Media/D-100", "scanner-new", 60, "newest snapshot", "metadata")
			backup("Media/D-200", "scanner-other", 120, "fallback snapshot")
			for index in (ds_path / "ns/Media/ns/D-100/host/scanner-old").glob("*/*.didx"):
				os.utime(index, (base_time + 100000, base_time + 100000))
			print("PASS: three real PBS backups uploaded in nested namespaces", flush=True)
			layout = {"template": "{resolve}/{ns.leaf}", "selection": "namespace", "resolver_url": f"http://127.0.0.1:{server.server_port}/paths", "windows_names": True}
			fields = {"datastore": name, "ns": "Media", "mode": "ro", "replace": "1", "layout": json.dumps(layout)}
			plans = api("d2d-mount-profiles/preview", "POST", fields)
			assert len(plans) == 2, plans
			selected = {p["group"]["namespace"]: p for p in plans}
			assert selected["Media/D-100"]["group"]["backup_id"] == "scanner-new", plans
			assert selected["Media/D-100"]["file_name"].endswith(".mpxar.didx"), plans
			assert selected["Media/D-200"]["file_name"].endswith(".pxar.didx"), plans
			expected = {"Project A/D-100": "newest snapshot", "Project B/D-100": "newest snapshot", "Uncategorized/D-200": "fallback snapshot"}
			assert {p for plan in plans for p in plan["paths"]} == set(expected), plans
			assert not sessions(), sessions()
			assert Resolver.requests and all(m["datastore"] == name for m in Resolver.requests)
			assert all(m["archive"].endswith(".didx") for m in Resolver.requests)
			print("PASS: preview chooses newest PBS time, fans out, deduplicates, falls back and mounts nothing", flush=True)
			legacy = dict(fields, layout="")
			assert len(api("d2d-mount-profiles/preview", "POST", legacy)) == 3
			collision = dict(fields, layout=json.dumps({"template": "same", "selection": "namespace"}))
			api("d2d-mount-profiles/preview", "POST", collision, status=400)
			print("PASS: legacy per-group planning preserved and collisions rejected", flush=True)

			for kind in ["local", "nfs", "s3-export"]:
				if kind == "local" and os.environ.get("PBS_LAYOUT_SKIP_FUSE") == "1":
					print("SKIP: local kernel mount explicitly disabled because /dev/fuse is unavailable", flush=True)
					continue
				params = dict(fields)
				outpost = ""
				if kind == "local":
					params["mount-path"] = str(work / "mounts")
				else:
					outpost = name + "-" + kind
					config = {"name": outpost, "type": kind, "listen-addr": f"127.0.0.1:{NFS_PORT if kind == 'nfs' else S3_PORT}"}
					if kind == "s3-export":
						config["s3-export"] = json.dumps({"tls": False, "region": "us-east-1", "credentials": [{"access_key": ACCESS, "secret_key": SECRET}]})
					api("d2d-outposts", "POST", config)
					outposts.append(outpost)
					params.update({"outpost": outpost, "share-name": "layout-data"})
				profile = api("d2d-mount-profiles", "POST", params)
				profile_id = profile["id"]
				profiles.append(profile_id)
				assert api("d2d-mount-profiles/" + profile_id)["layout"]["selection"] == "namespace"
				api("d2d-mount-profiles/" + profile_id + "/mount", "POST", {})
				wait(kind + " has exactly two mounted snapshot sessions for three paths", lambda: len(sessions()) == 2 and all(s["mounted"] for s in sessions()))
				assert {s["backup-id"] for s in sessions()} == {"scanner-new", "scanner-other"}
				for path, contents in expected.items():
					if kind == "local":
						assert (work / "mounts" / path / "marker.txt").read_text() == contents
					elif kind == "nfs":
						command("/tmp/layout-nfs-read", f"127.0.0.1:{NFS_PORT}", "layout-data", path + "/marker.txt", contents)
					else:
						assert s3("GET", path + "/marker.txt").decode() == contents
				print("PASS:", kind, "serves real backed-up bytes through every alias and fallback path", flush=True)
				if kind == "local":
					assert (work / "mounts/Project B/D-100").is_symlink()
					try:
						(work / "mounts/Project A/D-100/forbidden").write_text("no")
					except OSError as error:
						assert error.errno in [errno.EROFS, errno.EACCES], error
					else:
						raise AssertionError("local read-only mount accepted a write")
				if kind == "s3-export":
					listing = ET.fromstring(s3("GET", query={"list-type": "2"}))
					keys = {e.text for e in listing.iter() if e.tag.endswith("}Key")}
					assert keys == {p + "/marker.txt" for p in expected}, keys
					s3("PUT", "Project A/D-100/forbidden", body=b"no", status=403)
					before = sessions()
					Resolver.mode = "error"
					api("d2d-mount-profiles/preview", "POST", params, status=400)
					result = api("d2d-mount-profiles/" + profile_id + "/mount", "POST", {})
					upid = result
					def task_failed():
						return any("TASK ERROR" in p.read_text() for p in pathlib.Path("/var/log/proxmox-backup/tasks").glob("*/" + upid))
					wait("resolver failure is reported in the mount task", task_failed)
					assert sessions() == before
					assert s3("GET", "Project A/D-100/marker.txt") == b"newest snapshot"
					Resolver.mode = "normal"
					print("PASS: S3 list/read-only enforcement and resolver failure leaves exports intact", flush=True)
					backup("Media/D-100", "scanner-replacement", 180, "replacement snapshot")
					api("d2d-mount-profiles/" + profile_id + "/mount", "POST", {})
					wait("stale cross-host snapshot detached before replacement", lambda: all(s["backup-id"] != "scanner-new" for s in sessions()))
					api("d2d-mount-profiles/" + profile_id + "/mount", "POST", {})
					wait("newest snapshot from another host mounted", lambda: any(s["backup-id"] == "scanner-replacement" and s["mounted"] for s in sessions()))
					for prefix in ["Project A", "Project B"]:
						assert s3("GET", prefix + "/D-100/marker.txt") == b"replacement snapshot"
					print("PASS: newest-per-namespace replacement refreshes both S3 aliases", flush=True)
				detach_all()
				api("d2d-mount-profiles/" + profile_id, "DELETE")
				profiles.remove(profile_id)
				if outpost:
					api("d2d-outposts/" + outpost, "DELETE")
					outposts.remove(outpost)
			print("PROFILE_LAYOUT_E2E_PASSED", flush=True)
		finally:
			Resolver.mode = "normal"
			try:
				detach_all()
				for profile_id in profiles:
					api("d2d-mount-profiles/" + profile_id, "DELETE")
				for outpost in outposts:
					api("d2d-outposts/" + outpost, "DELETE")
			finally:
				server.shutdown()
				thread.join(timeout=5)
				server.server_close()
				if token is not None:
					command("proxmox-backup-manager", "acl", "update", "/datastore/" + name, "DatastoreAdmin", "--auth-id", token["tokenid"], "--delete", "true")
					command("proxmox-backup-manager", "user", "delete-token", "root@pam", name)
				if store_created:
					command("proxmox-backup-manager", "datastore", "remove", name)

if __name__ == "__main__":
	main()
