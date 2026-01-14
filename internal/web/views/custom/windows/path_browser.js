Ext.define("PBS.window.D2DPathSelector", {
  extend: "Proxmox.window.FileBrowser",
  alias: "widget.pbsD2DPathSelector",

  title: gettext("Select Path"),
  isFromRoot: true,

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
      let tree = me.lookup("tree");
      let selection = tree.getSelection();
      me.lookup("selectBtn").setDisabled(!selection || selection.length < 1);
    },

    onSelect: function () {
      let me = this;
      let tree = me.lookup("tree");
      let selection = tree.getSelection();
      if (selection && selection.length > 0) {
        let data = selection[0].data;
        try {
          let path = atob(data.filepath);
          if (me.isFromRoot) {
            if (!path.startsWith("/")) {
              path = "/" + path;
            }
          }
          me.getView().fireEvent("select", path);
          me.getView().close();
        } catch (e) {
          console.error("Failed to decode path:", data.filepath);
        }
      }
    },

    control: {
      treepanel: {
        selectionchange: "fileChanged",
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
