Ext.define("PBS.D2DRestore.DatastorePanel", {
  extend: "Ext.tree.Panel",
  alias: "widget.pbsPlusDatastorePanel",
  config: { datastore: null },
  mixins: ["Proxmox.Mixin.CBind"],

  rootVisible: false,

  title: gettext("Content"),

  controller: {
    xclass: "Ext.app.ViewController",

    init: function (view) {
      if (!view.datastore) {
        throw "no datastore specified";
      }

      this.store = Ext.create("Ext.data.Store", {
        model: "pbs-data-store-snapshots",
        groupField: "backup-group",
      });
      this.store.on("load", this.onLoad, this);

      view.getStore().setSorters(["sortWeight", "text", "backup-time"]);

      this.reload();
    },

    control: {
      "#": {
        // view
        rowdblclick: "rowDoubleClicked",
      },
      pbsNamespaceSelector: {
        change: "nsChange",
      },
    },

    rowDoubleClicked: function (table, rec, el, rowId, ev) {
      if (rec?.data?.ty === "ns" && !rec.data.root) {
        this.nsChange(null, rec.data.ns);
      }
    },

    nsChange: function (field, value) {
      let view = this.getView();
      if (field === null) {
        field = view.down("pbsNamespaceSelector");
        field.setValue(value);
        return;
      }
      view.namespace = value;
      this.reload();
    },

    reload: function () {
      let view = this.getView();

      if (!view.store || !this.store) {
        console.warn("cannot reload, no store(s)");
        return;
      }

      let url = `/api2/json/admin/datastore/${view.datastore}/snapshots`;
      if (view.namespace && view.namespace !== "") {
        url += `?ns=${encodeURIComponent(view.namespace)}`;
      }
      this.store.setProxy({
        type: "proxmox",
        timeout: 300 * 1000, // 5 minutes, we should make that api call faster
        url: url,
      });

      this.store.load();
    },

    unmountAll: function () {
      let me = this;
      let view = me.getView();

      let params = {};

      if (view.namespace && view.namespace !== "") {
        params.ns = view.namespace;
      }

      PBS.PlusUtils.API2Request({
        url:
          "/api2/extjs/config/d2d-unmount-all/" +
          encodeURIComponent(encodePathValue(view.datastore)),
        method: "POST",
        params,
        waitMsgTarget: view,
        failure: function (resp) {
          Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
        },
        success: function (resp) {
          Ext.toast(gettext("Unmount request sent"));
        },
      });
    },

    s3Refresh: function () {
      let me = this;
      let view = me.getView();
      Proxmox.Utils.API2Request({
        url: `/admin/datastore/${view.datastore}/s3-refresh`,
        method: "PUT",
        failure: (response) =>
          Ext.Msg.alert(gettext("Error"), response.htmlStatus),
        success: function (response, options) {
          Ext.create("Proxmox.window.TaskViewer", {
            upid: response.result.data,
            taskDone: () => me.reload(),
          }).show();
        },
      });
    },

    getRecordGroups: function (records) {
      let groups = {};

      for (const item of records) {
        let btype = item.data["backup-type"];
        let group = btype + "/" + item.data["backup-id"];

        if (groups[group] !== undefined) {
          continue;
        }

        let cls = PBS.Utils.get_type_icon_cls(btype);
        if (cls === "") {
          console.warn(`got unknown backup-type '${btype}'`);
          continue; // FIXME: auto render? what do?
        }

        groups[group] = {
          text: group,
          leaf: false,
          iconCls: "fa " + cls,
          expanded: false,
          backup_type: item.data["backup-type"],
          backup_id: item.data["backup-id"],
          children: [],
        };
      }

      return groups;
    },

    updateGroupNotes: async function (view) {
      try {
        let url = `/api2/extjs/admin/datastore/${view.datastore}/groups`;
        if (view.namespace && view.namespace !== "") {
          url += `?ns=${encodeURIComponent(view.namespace)}`;
        }
        let {
          result: { data: groups },
        } = await Proxmox.Async.api2({ url });
        let map = {};
        for (const group of groups) {
          map[`${group["backup-type"]}/${group["backup-id"]}`] = group.comment;
        }
        view.getRootNode().cascade((node) => {
          if (node.data.ty === "group") {
            let group = `${node.data.backup_type}/${node.data.backup_id}`;
            node.set("comment", map[group], { dirty: false });
          }
        });
      } catch (err) {
        console.debug(err);
      }
    },

    loadNamespaceFromSameLevel: async function () {
      let view = this.getView();
      try {
        let url = `/api2/extjs/admin/datastore/${view.datastore}/namespace?max-depth=1`;
        if (view.namespace && view.namespace !== "") {
          url += `&parent=${encodeURIComponent(view.namespace)}`;
        }
        let {
          result: { data: ns },
        } = await Proxmox.Async.api2({ url });
        return ns;
      } catch (err) {
        console.debug(err);
      }
      return [];
    },

    onLoad: async function (store, records, success, operation) {
      let me = this;
      let view = this.getView();

      let namespaces = await me.loadNamespaceFromSameLevel();

      if (!success) {
        // TODO also check error code for != 403 ?
        if (namespaces.length === 0) {
          let error = Proxmox.Utils.getResponseErrorMessage(
            operation.getError(),
          );
          Proxmox.Utils.setErrorMask(view.down("treeview"), error);
          return;
        } else {
          records = [];
        }
      } else {
        Proxmox.Utils.setErrorMask(view.down("treeview"));
      }

      let groups = this.getRecordGroups(records);

      let selected;
      let expanded = {};

      view.getSelection().some(function (item) {
        let id = item.data.text;
        if (item.data.leaf) {
          id = item.parentNode.data.text + id;
        }
        selected = id;
        return true;
      });

      view.getRootNode().cascadeBy({
        before: (item) => {
          if (item.isExpanded() && !item.data.leaf) {
            let id = item.data.text;
            expanded[id] = true;
            return true;
          }
          return false;
        },
        after: Ext.emptyFn,
      });

      for (const item of records) {
        let group = item.data["backup-type"] + "/" + item.data["backup-id"];
        let children = groups[group].children;

        let data = item.data;

        data.text =
          group + "/" + PBS.Utils.render_datetime_utc(data["backup-time"]);
        data.leaf = false;
        data.cls = "no-leaf-icons";
        data.matchesFilter = true;
        data.ty = "dir";

        data.expanded = !!expanded[data.text];

        data.children = [];
        for (const file of data.files) {
          file.text = file.filename;
          file["crypt-mode"] = PBS.Utils.cryptmap.indexOf(file["crypt-mode"]);
          file.fingerprint = data.fingerprint;
          file.leaf = true;
          file.matchesFilter = true;
          file.ty = "file";

          data.children.push(file);
        }

        children.push(data);
      }

      let nowSeconds = Date.now() / 1000;
      let children = [];
      for (const [name, group] of Object.entries(groups)) {
        let last_backup = 0;
        let crypt = {
          none: 0,
          mixed: 0,
          "sign-only": 0,
          encrypt: 0,
        };
        let verify = {
          outdated: 0,
          none: 0,
          failed: 0,
          ok: 0,
        };
        for (let item of group.children) {
          crypt[PBS.Utils.cryptmap[item["crypt-mode"]]]++;
          if (item["backup-time"] > last_backup && item.size !== null) {
            last_backup = item["backup-time"];
            group["backup-time"] = last_backup;
            group["last-comment"] = item.comment;
            group.files = item.files;
            group.size = item.size;
            group.owner = item.owner;
            verify.lastFailed =
              item.verification && item.verification.state !== "ok";
          }
          if (!item.verification) {
            verify.none++;
          } else {
            if (item.verification.state === "ok") {
              verify.ok++;
            } else {
              verify.failed++;
            }
            let task = Proxmox.Utils.parse_task_upid(item.verification.upid);
            item.verification.lastTime = task.starttime;
            if (nowSeconds - task.starttime > 30 * 24 * 60 * 60) {
              verify.outdated++;
            }
          }
        }
        group.verification = verify;
        group.count = group.children.length;
        group.matchesFilter = true;
        crypt.count = group.count;
        group["crypt-mode"] = PBS.Utils.calculateCryptMode(crypt);
        group.expanded = !!expanded[name];
        group.sortWeight = 0;
        group.ty = "group";
        children.push(group);
      }

      for (const item of namespaces) {
        if (item.ns === view.namespace || (!view.namespace && item.ns === "")) {
          continue;
        }
        children.push({
          text: item.ns,
          iconCls: "fa fa-object-group",
          expanded: true,
          expandable: false,
          ns: (view.namespaces ?? "") !== "" ? `/${item.ns}` : item.ns,
          ty: "ns",
          sortWeight: 10,
          leaf: true,
        });
      }

      let isRootNS = !view.namespace || view.namespace === "";
      let rootText = isRootNS
        ? gettext("Root Namespace")
        : Ext.String.format(gettext("Namespace '{0}'"), view.namespace);

      let topNodes = [];
      if (!isRootNS) {
        let parentNS = view.namespace.split("/").slice(0, -1).join("/");
        topNodes.push({
          text: `.. (${parentNS === "" ? gettext("Root") : parentNS})`,
          iconCls: "fa fa-level-up",
          ty: "ns",
          ns: parentNS,
          sortWeight: -10,
          leaf: true,
        });
      }
      topNodes.push({
        text: rootText,
        iconCls: "fa fa-" + (isRootNS ? "database" : "object-group"),
        expanded: true,
        expandable: false,
        sortWeight: -5,
        root: true, // fake root
        isRootNS,
        ty: "ns",
        children: children,
      });

      view.setRootNode({
        expanded: true,
        children: topNodes,
      });

      if (!children.length) {
        view.setEmptyText(
          Ext.String.format(
            gettext("No accessible snapshots found in namespace {0}"),
            view.namespace && view.namespace !== ""
              ? `'${view.namespace}'`
              : gettext("Root"),
          ),
        );
      }

      this.updateGroupNotes(view);

      if (selected !== undefined) {
        let selection = view.getRootNode().findChildBy(
          function (item) {
            let id = item.data.text;
            if (item.data.leaf) {
              id = item.parentNode.data.text + id;
            }
            return selected === id;
          },
          undefined,
          true,
        );
        if (selection) {
          view.setSelection(selection);
          view.getView().focusRow(selection);
        }
      }

      Proxmox.Utils.setErrorMask(view, false);
      if (view.getStore().getFilters().length > 0) {
        let searchBox = me.lookup("searchbox");
        let searchvalue = searchBox.getValue();
        me.search(searchBox, searchvalue);
      }
    },

    onCopy: async function (view, rI, cI, item, e, { data }) {
      await navigator.clipboard.writeText(data.text);
    },

    onNotesEdit: function (view, data) {
      let me = this;

      let isGroup = data.ty === "group";

      let params;
      if (isGroup) {
        params = {
          "backup-type": data.backup_type,
          "backup-id": data.backup_id,
        };
      } else {
        params = {
          "backup-type": data["backup-type"],
          "backup-id": data["backup-id"],
          "backup-time": (data["backup-time"].getTime() / 1000).toFixed(0),
        };
      }
      if (view.namespace && view.namespace !== "") {
        params.ns = view.namespace;
      }

      Ext.create("PBS.window.NotesEdit", {
        url: `/admin/datastore/${view.datastore}/${isGroup ? "group-notes" : "notes"}`,
        autoShow: true,
        apiCallDone: () => me.reload(), // FIXME: do something more efficient?
        extraRequestParams: params,
      });
    },

    mountBackup: function (tV, rI, cI, item, e, rec) {
      let me = this;
      let view = me.getView();

      if (!rec || rec.data.ty !== "file") {
        return;
      }

      let snapshot = rec.parentNode.data;
      let file = rec.data.filename;

      let isoTime = snapshot["backup-time"];
      if (isoTime instanceof Date) {
        isoTime = isoTime.toISOString().replace(/\.\d{3}Z$/, "Z"); // ensure ISO Z format
      }

      // Build params (no mount path anymore)
      let params = {
        "backup-id": snapshot["backup-id"],
        "backup-type": snapshot["backup-type"],
        "backup-time": isoTime,
        "file-name": file,
      };

      if (view.namespace && view.namespace !== "") {
        params.ns = view.namespace;
      }

      PBS.PlusUtils.API2Request({
        url:
          "/api2/extjs/config/d2d-mount/" +
          encodeURIComponent(encodePathValue(view.datastore)),
        method: "POST",
        params,
        waitMsgTarget: view,
        failure: function (resp) {
          Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
        },
        success: function (resp) {
          Ext.toast(gettext(`Backup mounted to /mnt/pbs-plus-restores`));
        },
      });
    },

    unmountBackup: function (tV, rI, cI, item, e, rec) {
      let me = this;
      let view = me.getView();

      let snapshot = rec && rec.parentNode ? rec.parentNode.data : null;
      let fileRec = rec && rec.data && rec.data.ty === "file" ? rec.data : null;

      if (!snapshot || !fileRec) {
        Ext.Msg.alert(
          gettext("Error"),
          gettext("Please select a file entry to unmount."),
        );
        return;
      }

      let isoTime = snapshot["backup-time"];
      if (isoTime instanceof Date) {
        isoTime = isoTime.toISOString().replace(/\.\d{3}Z$/, "Z"); // ensure ISO Z format
      }

      // Build params (no mount path anymore)
      let params = {
        "backup-id": snapshot["backup-id"],
        "backup-type": snapshot["backup-type"],
        "backup-time": isoTime,
        "file-name": fileRec.filename,
      };

      if (view.namespace && view.namespace !== "") {
        params.ns = view.namespace;
      }

      PBS.PlusUtils.API2Request({
        url:
          "/api2/extjs/config/d2d-unmount/" +
          encodeURIComponent(encodePathValue(view.datastore)),
        method: "POST",
        params,
        waitMsgTarget: view,
        failure: function (resp) {
          Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
        },
        success: function (resp) {
          Ext.toast(gettext("Unmount request sent"));
        },
      });
    },

    // opens either a namespace or a pxar file-browser
    openBrowser: function (tv, rI, Ci, item, e, rec) {
      let me = this;
      let view = me.getView();

      if (rec.data.ty === "ns") {
        me.nsChange(null, rec.data.ns);
        return;
      }
      if (rec?.data?.ty !== "file") {
        return;
      }
      let snapshot = rec.parentNode.data;

      let id = snapshot["backup-id"];
      let time = snapshot["backup-time"];
      let type = snapshot["backup-type"];
      let timetext = PBS.Utils.render_datetime_utc(snapshot["backup-time"]);
      let extraParams = {
        "backup-id": id,
        "backup-time": (time.getTime() / 1000).toFixed(0),
        "backup-type": type,
      };
      if (rec.data.filename.endsWith(".mpxar.didx")) {
        extraParams["archive-name"] = rec.data.filename;
      }
      if (view.namespace && view.namespace !== "") {
        extraParams.ns = view.namespace;
      }
      Ext.create("Proxmox.window.FileBrowser", {
        title: `${type}/${id}/${timetext}`,
        listURL: `/api2/json/admin/datastore/${view.datastore}/catalog`,
        downloadURL: `/api2/json/admin/datastore/${view.datastore}/pxar-file-download`,
        extraParams,
        enableTar: true,
        downloadPrefix: `${type}-${id}-`,
        archive: rec.data.filename,
      }).show();
    },

    filter: function (item, value) {
      if (item.data.text.indexOf(value) !== -1) {
        return true;
      }

      if (item.data.owner && item.data.owner.indexOf(value) !== -1) {
        return true;
      }

      return false;
    },

    search: function (tf, value) {
      let me = this;
      let view = me.getView();
      let store = view.getStore();
      if (!value && value !== 0) {
        store.clearFilter();
        // only collapse the children below our toplevel namespace "root"
        store.getRoot().lastChild.collapseChildren(true);
        tf.triggers.clear.setVisible(false);
        return;
      }
      tf.triggers.clear.setVisible(true);
      if (value.length < 2) {
        return;
      }
      Proxmox.Utils.setErrorMask(view, true);
      // we do it a little bit later for the error mask to work
      setTimeout(function () {
        store.clearFilter();
        store.getRoot().collapseChildren(true);

        store.beginUpdate();
        store.getRoot().cascadeBy({
          before: function (item) {
            if (me.filter(item, value)) {
              item.set("matchesFilter", true);
              if (item.parentNode && item.parentNode.id !== "root") {
                item.parentNode.childmatches = true;
              }
              return false;
            }
            return true;
          },
          after: function (item) {
            if (
              me.filter(item, value) ||
              item.id === "root" ||
              item.childmatches
            ) {
              item.set("matchesFilter", true);
              if (item.parentNode && item.parentNode.id !== "root") {
                item.parentNode.childmatches = true;
              }
              if (item.childmatches) {
                item.expand();
              }
            } else {
              item.set("matchesFilter", false);
            }
            delete item.childmatches;
          },
        });
        store.endUpdate();

        store.filter((item) => !!item.get("matchesFilter"));
        Proxmox.Utils.setErrorMask(view, false);
      }, 10);
    },
  },

  listeners: {
    activate: function () {
      let me = this;
      // only load on first activate to not load every tab switch
      if (!me.firstLoad) {
        me.getController().reload();
        me.firstLoad = true;
      }
    },
    itemcontextmenu: function (panel, record, item, index, event) {
      event.stopEvent();
      let menu;
      let view = panel.up("pbsDataStoreContent");
      let controller = view.getController();
      let createControllerCallback = function (name) {
        return function () {
          controller[name](
            view,
            undefined,
            undefined,
            undefined,
            undefined,
            record,
          );
        };
      };
      if (record.data.ty === "group") {
        menu = Ext.create("PBS.datastore.GroupCmdMenu", {
          title: gettext("Group"),
          onCopy: createControllerCallback("onCopy"),
        });
      } else if (record.data.ty === "dir") {
        menu = Ext.create("PBS.datastore.SnapshotCmdMenu", {
          title: gettext("Snapshot"),
          onCopy: createControllerCallback("onCopy"),
        });
      }
      if (menu) {
        menu.showAt(event.getXY());
      }
    },
  },

  viewConfig: {
    getRowClass: function (record, index) {
      let verify = record.get("verification");
      if (verify && verify.lastFailed) {
        return "proxmox-invalid-row";
      }
      return null;
    },
  },

  columns: [
    {
      xtype: "treecolumn",
      header: gettext("Backup Group"),
      dataIndex: "text",
      renderer: (value, meta, record) => {
        if (record.data.protected) {
          return `${value} (${gettext("protected")})`;
        }
        return value;
      },
      flex: 1,
    },
    {
      text: gettext("Comment"),
      dataIndex: "comment",
      flex: 1,
      renderer: (v, meta, record) => {
        let data = record.data;
        if (!data || data.leaf || data.root) {
          return "";
        }

        let additionalClasses = "";
        if (!v) {
          if (!data.expanded) {
            v = data["last-comment"] ?? "";
            additionalClasses = "pmx-opacity-75";
          } else {
            v = "";
          }
        }
        v = Ext.String.htmlEncode(v);
        let icon = "x-action-col-icon fa fa-fw fa-pencil pointer";

        return `<span class="snapshot-comment-column ${additionalClasses}">${v}</span>
		    <i data-qtip="${gettext("Edit")}" style="float: right; margin: 0px;" class="${icon}"></i>`;
      },
      listeners: {
        afterrender: function (component) {
          // a bit of a hack, but relatively easy, cheap and works out well.
          // more efficient to use one handler for the whole column than for each icon
          component.on("click", function (tree, cell, rowI, colI, e, rec) {
            let el = e.target;
            if (el.tagName !== "I" || !el.classList.contains("fa-pencil")) {
              return;
            }
            let view = tree.up();
            let controller = view.controller;
            controller.onNotesEdit(view, rec.data);
          });
        },
        dblclick: function (tree, el, row, col, ev, rec) {
          let data = rec.data || {};
          if (data.leaf || data.root) {
            return;
          }
          let view = tree.up();
          let controller = view.controller;
          controller.onNotesEdit(view, rec.data);
        },
      },
    },
    {
      header: gettext("Actions"),
      xtype: "actioncolumn",
      dataIndex: "text",
      width: 150,
      items: [
        {
          handler: "mountBackup",
          getTip: (v, m, rec) => Ext.String.format(gettext("Mount '{0}'"), v),
          getClass: (v, m, { data }) => {
            if (
              data.ty === "file" &&
              (data.filename.endsWith(".pxar.didx") ||
                data.filename.endsWith(".mpxar.didx"))
            ) {
              return "fa fa-hdd-o";
            }
            return "pmx-hidden";
          },
          isActionDisabled: (v, r, c, i, { data }) =>
            !(
              data.ty === "file" &&
              (data.filename.endsWith(".pxar.didx") ||
                data.filename.endsWith(".mpxar.didx")) &&
              data["crypt-mode"] < 3
            ),
        },
        {
          handler: "unmountBackup",
          getTip: (v, m, rec) => Ext.String.format(gettext("Unmount '{0}'"), v),
          getClass: (v, m, { data }) => {
            if (
              data.ty === "file" &&
              (data.filename.endsWith(".pxar.didx") ||
                data.filename.endsWith(".mpxar.didx"))
            ) {
              return "fa fa-eject";
            }
            return "pmx-hidden";
          },
          isActionDisabled: (v, r, c, i, { data }) =>
            !(
              data.ty === "file" &&
              (data.filename.endsWith(".pxar.didx") ||
                data.filename.endsWith(".mpxar.didx")) &&
              data["crypt-mode"] < 3
            ),
        },
        {
          handler: "openBrowser",
          tooltip: gettext("Browse"),
          getClass: (v, m, { data }) => {
            if (
              (data.ty === "file" &&
                (data.filename.endsWith(".pxar.didx") ||
                  data.filename.endsWith(".mpxar.didx"))) ||
              (data.ty === "ns" && !data.root)
            ) {
              return "fa fa-folder-open-o";
            }
            return "pmx-hidden";
          },
          isActionDisabled: (v, r, c, i, { data }) =>
            !(
              data.ty === "file" &&
              (data.filename.endsWith(".pxar.didx") ||
                data.filename.endsWith(".mpxar.didx")) &&
              data["crypt-mode"] < 3
            ) && data.ty !== "ns",
        },
      ],
    },
    {
      xtype: "datecolumn",
      header: gettext("Backup Time"),
      sortable: true,
      dataIndex: "backup-time",
      format: "Y-m-d H:i:s",
      width: 150,
    },
    {
      header: gettext("Size"),
      sortable: true,
      dataIndex: "size",
      renderer: (v, meta, { data }) => {
        if (
          (data.text === "client.log.blob" && v === undefined) ||
          (data.ty !== "dir" && data.ty !== "file")
        ) {
          return "";
        }
        if (v === undefined || v === null) {
          meta.tdCls = "x-grid-row-loading";
          return "";
        }
        return Proxmox.Utils.format_size(v);
      },
    },
    {
      xtype: "numbercolumn",
      format: "0",
      header: gettext("Count"),
      sortable: true,
      width: 75,
      align: "right",
      dataIndex: "count",
    },
    {
      header: gettext("Encrypted"),
      dataIndex: "crypt-mode",
      renderer: (v, meta, record) => {
        if (record.data.size === undefined || record.data.size === null) {
          return "";
        }
        if (v === -1) {
          return "";
        }
        let iconCls = PBS.Utils.cryptIconCls[v] || "";
        let iconTxt = "";
        if (iconCls) {
          iconTxt = `<i class="fa fa-fw fa-${iconCls}"></i> `;
        }
        let tip;
        if (
          v !== PBS.Utils.cryptmap.indexOf("none") &&
          record.data.fingerprint !== undefined
        ) {
          tip = "Key: " + PBS.Utils.renderKeyID(record.data.fingerprint);
        }
        let txt = iconTxt + PBS.Utils.cryptText[v] || Proxmox.Utils.unknownText;
        if (record.data.ty === "group" || tip === undefined) {
          return txt;
        } else {
          return `<span data-qtip="${tip}">${txt}</span>`;
        }
      },
    },
  ],

  initComponent: function () {
    let me = this;

    me.callParent();

    Proxmox.Utils.API2Request({
      url: `/config/datastore/${me.datastore}`,
      failure: (response) =>
        Ext.Msg.alert(gettext("Error"), response.htmlStatus),
      success: function (response, options) {
        let data = response.result.data;
        if (data.backend) {
          let backendConfig = PBS.Utils.parsePropertyString(data.backend);
          let hasS3Backend = backendConfig.type === "s3";
          me.down("#moreDropdown").setHidden(!hasS3Backend);
        }
      },
    });
  },

  tbar: [
    {
      text: gettext("Reload"),
      iconCls: "fa fa-refresh",
      handler: "reload",
    },
    {
      text: gettext("Unmount All"),
      iconCls: "fa fa-eject",
      handler: "unmountAll",
    },
    {
      text: gettext("More"),
      itemId: "moreDropdown",
      hidden: true,
      menu: [
        {
          text: gettext("Refresh contents from S3 bucket"),
          iconCls: "fa fa-cloud-download",
          handler: "s3Refresh",
          selModel: false,
        },
      ],
    },
    "->",
    {
      xtype: "tbtext",
      html: gettext("Namespace") + ":",
    },
    {
      xtype: "pbsNamespaceSelector",
      width: 200,
      cbind: {
        datastore: "{datastore}",
      },
    },
    "-",
    {
      xtype: "tbtext",
      html: gettext("Search"),
    },
    {
      xtype: "textfield",
      reference: "searchbox",
      emptyText: gettext("group, date or owner"),
      triggers: {
        clear: {
          cls: "pmx-clear-trigger",
          weight: -1,
          hidden: true,
          handler: function () {
            this.triggers.clear.setVisible(false);
            this.setValue("");
          },
        },
      },
      listeners: {
        change: {
          fn: "search",
          buffer: 500,
        },
      },
    },
  ],
});
