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

function encodePathValue(path) {
  const encoded = btoa(path)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
  return encoded;
}

function humanReadableBytes(bytes) {
  const KB = 1024;
  const MB = KB * 1024;
  const GB = MB * 1024;
  const TB = GB * 1024;

  if (bytes >= TB) {
    return `${(bytes / TB).toFixed(2)} TB`;
  } else if (bytes >= GB) {
    return `${(bytes / GB).toFixed(2)} GB`;
  } else if (bytes >= MB) {
    return `${(bytes / MB).toFixed(2)} MB`;
  } else if (bytes >= KB) {
    return `${(bytes / KB).toFixed(2)} KB`;
  } else {
    return `${bytes} B`;
  }
}

function humanReadableSpeed(speed) {
  const KB = 1024.0;
  const MB = KB * 1024;
  const GB = MB * 1024;

  if (speed >= GB) {
    return `${(speed / GB).toFixed(2)} GB/s`;
  } else if (speed >= MB) {
    return `${(speed / MB).toFixed(2)} MB/s`;
  } else if (speed >= KB) {
    return `${(speed / KB).toFixed(2)} KB/s`;
  } else {
    return `${speed.toFixed(2)} B/s`;
  }
}
