const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

let definition;
const requests = [];
let confirmed;
const Ext = {
  define: (name, config) => {
    if (name === "PBS.D2DManagement.ScriptPanel") definition = config;
  },
  apply: Object.assign,
  String: {
    htmlEncode: (value) => String(value),
    format: (template, ...args) =>
      template.replace(/\{(\d+)\}/g, (_, index) => args[index]),
  },
  Msg: {
    confirm: (title, message, callback) => {
      confirmed = { title, message, callback };
    },
    alert: () => {},
  },
};
vm.runInNewContext(fs.readFileSync(process.argv[2], "utf8"), {
  Ext,
  gettext: (value) => value,
  encodePathValue: (value) => value,
  pbsPlusBaseUrl: "https://localhost:8017",
  Proxmox: { Utils: { format_size: String } },
  PBS: {
    PlusUtils: { API2Request: (request) => requests.push(request) },
  },
});

assert.ok(definition, "script panel definition missing");
const methods = definition.controller;
assert.ok(methods.removeScripts, "removeScripts handler missing");

let selection = [];
let reloaded = 0;
const controller = {
  getView: () => ({ getSelection: () => selection }),
  reload: () => {
    reloaded += 1;
  },
  removeScripts: methods.removeScripts,
};

const record = (id, jobs, targets) => ({
  getId: () => id,
  get: (field) => (field === "job_count" ? jobs : targets),
});

controller.removeScripts();
assert.equal(confirmed, undefined, "empty selection must not prompt");

selection = [record("/scripts/unused.sh", 0, 0)];
controller.removeScripts();
assert.equal(confirmed.message, "Remove script '/scripts/unused.sh'?");

confirmed.callback("no");
assert.equal(requests.length, 0, "declining must not delete");

confirmed.callback("yes");
assert.equal(requests.length, 1);
assert.equal(
  requests[0].url,
  "/api2/extjs/config/d2d-script/" + encodeURIComponent("/scripts/unused.sh"),
);
assert.equal(requests[0].method, "DELETE");
requests[0].success();
assert.equal(reloaded, 1);

selection = [record("/scripts/used.sh", 3, 1)];
controller.removeScripts();
assert.equal(
  confirmed.message,
  "Script '/scripts/used.sh' is still used by 3 backup job(s) and 1 target(s). Remove it anyway?",
);

selection = [record("/scripts/a.sh", 1, 0), record("/scripts/b.sh", 0, 2)];
controller.removeScripts();
assert.equal(
  confirmed.message,
  "Script '/scripts/a.sh', '/scripts/b.sh' is still used by 1 backup job(s) and 2 target(s). Remove it anyway?",
);
confirmed.callback("yes");
assert.equal(requests.length, 3);

console.log("Script removal prompt checks passed");
