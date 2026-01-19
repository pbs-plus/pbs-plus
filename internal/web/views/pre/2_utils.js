Ext.define("PBS.PlusUtils", {
  singleton: true,

  LoadCodeMirror: function (callback) {
    if (window.CodeMirror) {
      callback();
      return;
    }

    let loadCSS = (href) => {
      return new Promise((resolve) => {
        let link = document.createElement("link");
        link.rel = "stylesheet";
        link.href = href;
        link.onload = resolve;
        document.head.appendChild(link);
      });
    };

    let loadJS = (src) => {
      return new Promise((resolve) => {
        let script = document.createElement("script");
        script.src = src;
        script.onload = resolve;
        document.head.appendChild(script);
      });
    };

    let baseURL = "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16";

    Promise.all([
      loadCSS(`${baseURL}/codemirror.min.css`),
      loadJS(`${baseURL}/codemirror.min.js`),
    ])
      .then(() => loadJS(`${baseURL}/mode/shell/shell.min.js`))
      .then(callback);
  },

  API2Request: function (reqOpts) {
    var newopts = Ext.apply(
      {
        withCredentials: true,
        cors: true,
        useDefaultXhrHeader: false,
        waitMsg: gettext("Please wait..."),
      },
      reqOpts,
    );

    // default to enable if user isn't handling the failure already explicitly
    let autoErrorAlert =
      reqOpts.autoErrorAlert ??
      (typeof reqOpts.failure !== "function" &&
        typeof reqOpts.callback !== "function");

    if (!newopts.url.match(/^\/api2/)) {
      newopts.url = "/api2/extjs" + newopts.url;
    }

    newopts.url = pbsPlusBaseUrl + newopts.url;

    delete newopts.callback;
    let unmask = (target) => {
      if (
        target.waitMsgTargetCount === undefined ||
        --target.waitMsgTargetCount <= 0
      ) {
        target.setLoading(false);
        delete target.waitMsgTargetCount;
      }
    };

    let createWrapper = function (successFn, callbackFn, failureFn) {
      Ext.apply(newopts, {
        success: function (response, options) {
          if (options.waitMsgTarget) {
            if (Proxmox.Utils.toolkit === "touch") {
              options.waitMsgTarget.setMasked(false);
            } else {
              unmask(options.waitMsgTarget);
            }
          }
          let result = Ext.decode(response.responseText);
          response.result = result;
          if (!result.success) {
            response.htmlStatus = Proxmox.Utils.extractRequestError(
              result,
              true,
            );
            Ext.callback(callbackFn, options.scope, [options, false, response]);
            Ext.callback(failureFn, options.scope, [response, options]);
            if (autoErrorAlert) {
              Ext.Msg.alert(gettext("Error"), response.htmlStatus);
            }
            return;
          }
          Ext.callback(callbackFn, options.scope, [options, true, response]);
          Ext.callback(successFn, options.scope, [response, options]);
        },
        failure: function (response, options) {
          if (options.waitMsgTarget) {
            if (Proxmox.Utils.toolkit === "touch") {
              options.waitMsgTarget.setMasked(false);
            } else {
              unmask(options.waitMsgTarget);
            }
          }
          response.result = {};
          try {
            response.result = Ext.decode(response.responseText);
          } catch (_e) {
            // ignore
          }
          let msg = gettext("Connection error") + " - server offline?";
          if (response.aborted) {
            msg = gettext("Connection error") + " - aborted.";
          } else if (response.timedout) {
            msg = gettext("Connection error") + " - Timeout.";
          } else if (response.status && response.statusText) {
            msg =
              gettext("Connection error") +
              " " +
              response.status +
              ": " +
              response.statusText;
          }
          response.htmlStatus = Ext.htmlEncode(msg);
          Ext.callback(callbackFn, options.scope, [options, false, response]);
          Ext.callback(failureFn, options.scope, [response, options]);
        },
      });
    };

    createWrapper(reqOpts.success, reqOpts.callback, reqOpts.failure);

    let target = newopts.waitMsgTarget;
    if (target) {
      if (Proxmox.Utils.toolkit === "touch") {
        target.setMasked({ xtype: "loadmask", message: newopts.waitMsg });
      } else if (target.rendered) {
        target.waitMsgTargetCount = (target.waitMsgTargetCount ?? 0) + 1;
        target.setLoading(newopts.waitMsg);
      } else {
        target.waitMsgTargetCount = (target.waitMsgTargetCount ?? 0) + 1;
        target.on(
          "afterlayout",
          function () {
            if ((target.waitMsgTargetCount ?? 0) > 0) {
              target.setLoading(newopts.waitMsg);
            }
          },
          target,
          { single: true },
        );
      }
    }
    Ext.Ajax.request(newopts);
  },

  render_task_status: function (
    value,
    metadata,
    record,
    rowIndex,
    colIndex,
    store,
  ) {
    if (
      !record.data["last-run-upid"] &&
      !store.getById("last-run-upid")?.data.value &&
      !record.data.upid &&
      !store.getById("upid")?.data.value
    ) {
      return "-";
    }

    if (
      !record.data["last-run-endtime"] &&
      !store.getById("last-run-endtime")?.data.value &&
      !record.data.endtime &&
      !store.getById("endtime")?.data.value
    ) {
      metadata.tdCls = "x-grid-row-loading";
      return "";
    }

    let parse_task_status = function (status) {
      if (status === "OK") {
        return "ok";
      }

      if (status === "unknown") {
        return "unknown";
      }

      let match = status.match(/^WARNINGS: (.*)$/);
      if (match) {
        return "warning";
      }

      match = status.match(/^QUEUED: (.*)$/);
      if (match) {
        return "queued";
      }

      return "error";
    };

    let parsed = parse_task_status(value);
    let text = value;
    let icon = "";
    switch (parsed) {
      case "unknown":
        icon = "question faded";
        text = Proxmox.Utils.unknownText;
        break;
      case "error":
        icon = "times critical";
        text = Proxmox.Utils.errorText + ": " + value;
        break;
      case "warning":
        icon = "exclamation warning";
        break;
      case "ok":
        icon = "check good";
        text = gettext("OK");
        break;
      case "queued":
        icon = "tasks faded";
        break;
    }

    return `<i class="fa fa-${icon}"></i> ${text}`;
  },
});

Ext.define("PBS.PlusRestProxy", {
  extend: "Proxmox.RestProxy",
  alias: "proxy.pbsplus",

  // Inherit all the original properties
  pageParam: null,
  startParam: null,
  limitParam: null,
  groupParam: null,
  sortParam: null,
  filterParam: null,
  noCache: false,

  // Keep the original afterRequest method
  afterRequest: function (request, success) {
    this.fireEvent("afterload", this, request, success);
  },

  constructor: function (config) {
    // Add CORS credentials configuration for cross-origin requests
    config = Ext.apply(
      {
        withCredentials: true,
        cors: true,
        useDefaultXhrHeader: false,
      },
      config,
    );

    // Apply the original reader configuration
    Ext.applyIf(config, {
      reader: {
        responseType: undefined,
        type: "json",
        rootProperty: config.root || "data",
      },
    });

    this.callParent([config]);
  },
});

Ext.define("PBS.plusWindow.Edit", {
  extend: "Proxmox.window.Edit",
  alias: "widget.pbsPlusWindowEdit",
  submit: function () {
    let me = this;

    let form = me.formPanel.getForm();

    let values = me.getValues();
    Ext.Object.each(values, function (name, val) {
      if (Object.hasOwn(values, name)) {
        if (Ext.isArray(val) && !val.length) {
          values[name] = "";
        }
      }
    });

    if (me.digest) {
      values.digest = me.digest;
    }

    if (me.backgroundDelay) {
      values.background_delay = me.backgroundDelay;
    }

    let url = Ext.isFunction(me.submitUrl)
      ? me.submitUrl(me.url, values)
      : me.submitUrl || me.url;
    if (me.method === "DELETE") {
      url = url + "?" + Ext.Object.toQueryString(values);
      values = undefined;
    }

    let requestOptions = Ext.apply(
      {
        url: url,
        waitMsgTarget: me,
        method: me.method || (me.backgroundDelay ? "POST" : "PUT"),
        params: values,
        failure: function (response, options) {
          me.apiCallDone(false, response, options);

          if (response.result && response.result.errors) {
            form.markInvalid(response.result.errors);
          }
          Ext.Msg.alert(gettext("Error"), response.htmlStatus);
        },
        success: function (response, options) {
          let hasProgressBar =
            (me.backgroundDelay || me.showProgress || me.showTaskViewer) &&
            response.result.data;

          me.apiCallDone(true, response, options);

          if (hasProgressBar) {
            // only hide to allow delaying our close event until task is done
            me.hide();

            let upid = response.result.data;
            let viewerClass = me.showTaskViewer ? "Viewer" : "Progress";
            Ext.create("Proxmox.window.Task" + viewerClass, {
              autoShow: true,
              upid: upid,
              taskDone: me.taskDone,
              listeners: {
                destroy: function () {
                  me.close();
                },
              },
            });
          } else {
            me.close();
          }
        },
      },
      me.submitOptions ?? {},
    );
    PBS.PlusUtils.API2Request(requestOptions);
  },

  load: function (options) {
    let me = this;

    let form = me.formPanel.getForm();

    options = options || {};

    let newopts = Ext.apply(
      {
        waitMsgTarget: me,
      },
      options,
    );

    if (Object.keys(me.extraRequestParams).length > 0) {
      let params = newopts.params || {};
      Ext.applyIf(params, me.extraRequestParams);
      newopts.params = params;
    }

    let url = Ext.isFunction(me.loadUrl)
      ? me.loadUrl(me.url, me.initialConfig)
      : me.loadUrl || me.url;

    let createWrapper = function (successFn) {
      Ext.apply(newopts, {
        url: url,
        method: "GET",
        success: function (response, opts) {
          form.clearInvalid();
          me.digest = response.result?.digest || response.result?.data?.digest;
          if (successFn) {
            successFn(response, opts);
          } else {
            me.setValues(response.result.data);
          }
          // hack: fix ExtJS bug
          Ext.Array.each(me.query("radiofield"), (f) => f.resetOriginalValue());
        },
        failure: function (response, opts) {
          Ext.Msg.alert(gettext("Error"), response.htmlStatus, function () {
            me.close();
          });
        },
      });
    };

    createWrapper(options.success);

    PBS.PlusUtils.API2Request(newopts);
  },
});

Ext.define("PBS.plusWindow.Create", {
  extend: "Proxmox.window.Edit",
  alias: "widget.pbsPlusWindowCreate",

  method: "POST",

  load: function () {
    let me = this;
    let form = me.formPanel.getForm();
    form.clearInvalid();
  },

  submit: function () {
    let me = this;
    let form = me.formPanel.getForm();
    let values = me.getValues();

    Ext.Object.each(values, function (name, val) {
      if (Object.hasOwn(values, name)) {
        if (Ext.isArray(val) && !val.length) {
          values[name] = "";
        }
      }
    });

    if (me.digest) {
      values.digest = me.digest;
    }

    if (me.backgroundDelay) {
      values.background_delay = me.backgroundDelay;
    }

    let url = Ext.isFunction(me.submitUrl)
      ? me.submitUrl(me.url, values)
      : me.submitUrl || me.url;

    if (me.method === "DELETE") {
      url = url + "?" + Ext.Object.toQueryString(values);
      values = undefined;
    }

    let requestOptions = Ext.apply(
      {
        url: url,
        waitMsgTarget: me,
        method: me.method || "POST",
        params: values,
        failure: function (response, options) {
          me.apiCallDone(false, response, options);
          if (response.result && response.result.errors) {
            form.markInvalid(response.result.errors);
          }
          Ext.Msg.alert(gettext("Error"), response.htmlStatus);
        },
        success: function (response, options) {
          let hasProgressBar =
            (me.backgroundDelay || me.showProgress || me.showTaskViewer) &&
            response.result.data;

          me.apiCallDone(true, response, options);

          if (hasProgressBar) {
            me.hide();
            let upid = response.result.data;
            let viewerClass = me.showTaskViewer ? "Viewer" : "Progress";
            Ext.create("Proxmox.window.Task" + viewerClass, {
              autoShow: true,
              upid: upid,
              taskDone: me.taskDone,
              listeners: {
                destroy: function () {
                  me.close();
                },
              },
            });
          } else {
            me.close();
          }
        },
      },
      me.submitOptions ?? {},
    );

    PBS.PlusUtils.API2Request(requestOptions);
  },
});
