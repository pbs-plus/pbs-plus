Ext.define("PBS.window.D2DPathSelector", {
  extend: "Proxmox.window.FileBrowser",
  alias: "widget.pbsD2DPathSelector",

  title: gettext("Select Path"),

  config: {
    prependSlash: true,
    onlyDirs: false,
  },

  controller: {
    xclass: "Ext.app.ViewController",

    init: function (view) {
      let me = this;
      let tree = me.lookup("tree");

      if (!view.listURL) {
        throw "no list URL given";
      }

      let store = tree.getStore();
      let proxy = store.getProxy();

      let errorCallback = (error, msg) => me.errorHandler(error, msg);
      proxy.setUrl(view.listURL);
      proxy.setTimeout(60 * 1000);
      proxy.setExtraParams(view.extraParams);
      proxy.setWithCredentials(true);

      proxy.setHeaders(
        Ext.apply(proxy.getHeaders() || {}, {
          Accept: "application/json",
        }),
      );

      tree.mon(store, "beforeload", () => {
        Proxmox.Utils.setErrorMask(tree, true);
      });

      tree.mon(store, "load", (treestore, rec, success, operation, node) => {
        if (success) {
          Proxmox.Utils.setErrorMask(tree, false);
          return;
        }
        if (operation?.error?.status === 503 && node.loadCount < 10) {
          node.collapse();
          node.expand();
          node.loadCount = (node.loadCount || 0) + 1;
          return;
        }

        let error = operation.getError();
        let msg = Proxmox.Utils.getResponseErrorMessage(error);
        if (!errorCallback(error, msg)) {
          Proxmox.Utils.setErrorMask(tree, msg);
        } else {
          Proxmox.Utils.setErrorMask(tree, false);
        }
      });

      store.load((rec, op, success) => {
        let root = store.getRoot();
        root.expand();
        if (root.childNodes.length === 1) {
          root.firstChild.expand();
        }
        me.initialLoadDone = success;
      });
    },

    errorHandler: function (error, msg) {
      let me = this;
      if (error?.status === 503) return false;
      if (me.initialLoadDone) {
        Ext.Msg.alert(gettext("Error"), msg);
        return true;
      }
      return false;
    },

    fileChanged: function () {
      let me = this;
      let view = me.getView();
      let tree = me.lookup("tree");
      let selection = tree.getSelection();

      let canSelect = selection && selection.length > 0;

      if (canSelect && view.getOnlyDirs()) {
        let rec = selection[0];
        if (rec.get("leaf") === true) {
          canSelect = false;
        }
      }

      me.lookup("selectBtn").setDisabled(!canSelect);
    },

    onSelect: function () {
      let me = this;
      let view = me.getView();
      let tree = me.lookup("tree");
      let selection = tree.getSelection();

      if (selection && selection.length > 0) {
        let rec = selection[0];

        if (view.getOnlyDirs() && rec.get("leaf") === true) {
          return;
        }

        let data = rec.data;
        try {
          let path = atob(data.filepath);

          if (view.getPrependSlash() && !path.startsWith("/")) {
            path = "/" + path;
          }

          view.fireEvent("select", path);
          view.close();
        } catch (e) {
          console.error("Failed to decode path:", data.filepath);
        }
      }
    },

    control: {
      treepanel: {
        selectionchange: "fileChanged",
        itemdblclick: function (v, rec) {
          if (!this.getView().getOnlyDirs() || rec.get("leaf") !== true) {
            this.onSelect();
          }
        },
      },
    },
  },

  fbar: [
    {
      xtype: "button",
      text: gettext("Select"),
      reference: "selectBtn",
      disabled: true,
      handler: "onSelect",
    },
    {
      xtype: "button",
      text: gettext("Cancel"),
      handler: function () {
        this.up("window").close();
      },
    },
  ],
});
