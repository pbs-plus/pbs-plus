const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const pending = [];
let definition;
const Ext = {
  define: (name, config) => {
    definition = config;
  },
  apply: Object.assign,
  decode: JSON.parse,
  Ajax: { request: (request) => pending.push(request) },
};
vm.runInNewContext(fs.readFileSync(process.argv[2], "utf8"), {
  Ext,
  pbsPlusBaseUrl: "https://localhost:8017",
  Proxmox: { Utils: { format_size: String } },
});
const kind = process.argv[3];
const searchField = {
  value: "",
  setValue(value) {
    this.value = value;
  },
};
const makeNode = (data) => {
  const node = {
    data,
    children: (data.children || []).map(makeNode),
    cascadeBy(callback) {
      callback(this);
      this.children.forEach((child) => child.cascadeBy(callback));
    },
  };
  return node;
};
const view = {
  targetKind: ["local", "agent"].includes(kind) ? "filesystem" : kind,
  setLoading(value) {
    this.loading = value;
  },
  setRootNode(data) {
    this.root = makeNode(data);
  },
  getRootNode() {
    return this.root;
  },
  down: () => searchField,
};
const controller = Object.assign({}, definition, {
  getView: () => view,
  loadStatuses() {},
});
const target = (name) => ({
  name,
  text: name,
  path: "/mnt/" + name,
  kind: ["local", "agent"].includes(kind) ? "filesystem" : kind,
  target_type: ["local", "agent", "s3"].includes(kind) ? kind : "database",
  agent_hostname: kind === "agent" ? "agent-host" : undefined,
  isGroup: false,
  leaf: true,
});
const respond = (names) => {
  assert.equal(pending.length, 1, "a reload should issue one request");
  pending.shift().success({
    responseText: JSON.stringify({
      data: [
        {
          text: "Targets",
          isGroup: true,
          expanded: true,
          children: names.map(target),
        },
      ],
    }),
  });
  assert.equal(
    pending.length,
    0,
    "applying a filter must not start another reload",
  );
};
const visible = () => {
  const names = [];
  view.root.cascadeBy((node) => {
    if (node.data.name) names.push(node.data.name);
  });
  return names.sort();
};
controller.loadData();
respond(["alpha-delete", "alpha-keep", "beta"]);
controller.onSearch(searchField, "ALPHA");
assert.deepEqual(visible(), ["alpha-delete", "alpha-keep"]);
controller.reload();
respond(["alpha-keep", "beta"]);
assert.equal(controller.searchValue, "alpha");
assert.deepEqual(
  visible(),
  ["alpha-keep"],
  "deleting one match must keep other results filtered",
);
controller.reload();
respond(["alpha-keep", "alpha-new", "beta", "unrelated"]);
assert.deepEqual(
  visible(),
  ["alpha-keep", "alpha-new"],
  "manual refresh must filter new records too",
);
controller.reload();
controller.onSearch(searchField, "beta");
respond(["alpha-keep", "beta", "beta-new"]);
assert.deepEqual(
  visible(),
  ["beta", "beta-new"],
  "use the latest query when an in-flight reload completes",
);
controller.reload();
respond(["alpha-keep"]);
assert.deepEqual(
  visible(),
  [],
  "deleting the last match must leave an empty filtered list",
);
controller.onClearSearch();
respond(["alpha-keep", "unrelated"]);
assert.equal(searchField.value, "");
assert.deepEqual(
  visible(),
  ["alpha-keep", "unrelated"],
  "clearing search must restore the full list",
);
console.log(
  kind +
    ": target search survives deletion, refresh, query changes and empty results",
);
