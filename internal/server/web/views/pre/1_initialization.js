const pbsFullUrl = window.location.href;
const pbsUrl = new URL(pbsFullUrl);
const pbsPlusBaseUrl = `${pbsUrl.protocol}//${pbsUrl.hostname}:8017`;

function encodePathValue(path) {
  const encoded = btoa(path)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
  return encoded;
}

