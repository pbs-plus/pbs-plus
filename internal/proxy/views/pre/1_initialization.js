const pbsFullUrl = window.location.href;
const pbsUrl = new URL(pbsFullUrl);
const pbsPlusBaseUrl = `${pbsUrl.protocol}//${pbsUrl.hostname}:8008`;

function getCookie(cName) {
	const name = cName + "=";
  const cDecoded = decodeURIComponent(document.cookie);
  const cArr = cDecoded.split('; ');
  let res;
  cArr.forEach(val => {
    if (val.indexOf(name) === 0) res = val.substring(name.length);
  })
  return res
}

var pbsPlusTokenHeaders = {
	"Content-Type": "application/json",
};

if (Proxmox.CSRFPreventionToken) {
	pbsPlusTokenHeaders["Csrfpreventiontoken"] = Proxmox.CSRFPreventionToken;
}

const refreshPlusToken = async () => {
  // Function to check if cookie exists
  const checkReady = () => {
		const cookie = getCookie("PBSAuthCookie");
		const csrfToken = pbsPlusTokenHeaders?.["Csrfpreventiontoken"];
		return cookie && csrfToken;
  };

  // Wait until cookie is available
  while (!checkReady()) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Make request once cookie exists
  return fetch(pbsPlusBaseUrl + "/plus/token", {
    method: "POST",
    body: JSON.stringify({
      "pbs_auth_cookie": getCookie("PBSAuthCookie"),
    }),
    headers: pbsPlusTokenHeaders,
  });
}

refreshPlusToken();

function encodePathValue(path) {
  const encoded = btoa(path)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
  return encoded;
}
