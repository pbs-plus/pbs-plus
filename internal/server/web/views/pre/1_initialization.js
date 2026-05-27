const pbsFullUrl = window.location.href;
const pbsUrl = new URL(pbsFullUrl);
const pbsPlusBaseUrl = `${pbsUrl.protocol}//${pbsUrl.hostname}:8017`;

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

