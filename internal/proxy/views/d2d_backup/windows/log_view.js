Ext.define('PBS.plusPanel.LogView', {
  extend: 'Ext.panel.Panel',
  xtype: 'proxmoxPlusLogView',

  pageSize: 510,
  viewBuffer: 50,
  lineHeight: 16,

  scrollToEnd: true,

  // callback for load failure, used for ceph
  failCallback: undefined,

  controller: {
    xclass: 'Ext.app.ViewController',

    updateParams: function () {
      let me = this;
      let viewModel = me.getViewModel();

      if (viewModel.get('hide_timespan') || viewModel.get('livemode')) {
        return;
      }

      let since = viewModel.get('since');
      let until = viewModel.get('until');

      if (since > until) {
        Ext.Msg.alert(
          'Error',
          'Since date must be less equal than Until date.'
        );
        return;
      }

      let submitFormat = viewModel.get('submitFormat');

      viewModel.set(
        'params.since',
        Ext.Date.format(since, submitFormat)
      );
      if (submitFormat === 'Y-m-d') {
        viewModel.set(
          'params.until',
          Ext.Date.format(until, submitFormat) + ' 23:59:59'
        );
      } else {
        viewModel.set('params.until', Ext.Date.format(until, submitFormat));
      }

      me.getView().loadTask.delay(200);
    },

    scrollPosBottom: function () {
      let view = this.getView();
      let pos = view.getScrollY();
      let maxPos = view.getScrollable().getMaxPosition().y;
      return maxPos - pos;
    },

    /**
     * Instead of building one huge content string whose length equals the log’s
     * total number of lines, we build a virtualized view.
     *
     * We create a spacer div at the top whose height is:
     *    startLine * lineHeight,
     * then list our fetched (visible) lines and add
     * a spacer at the bottom to simulate the remaining lines.
     */
    updateView: function (fetchedLines, startLine, total) {
      let me = this;
      let view = me.getView();
      let viewModel = me.getViewModel();
      let content = me.lookup('content');
      let data = viewModel.get('data');

      // Avoid needless DOM updates
      if (
        startLine === data.start &&
        total === data.total &&
        fetchedLines.length === data.fetchedLines
      ) {
        if (total !== 1) {
          return;
        }
      }
      viewModel.set('data', {
        start: startLine,
        total: total,
        fetchedLines: fetchedLines.length,
      });

      let scrollPos = me.scrollPosBottom();
      let scrollToBottom = view.scrollToEnd && scrollPos <= 5;

      // Build spacer divs to simulate the full log file height.
      let aboveHeight = startLine * view.lineHeight;
      let belowCount = total - startLine - fetchedLines.length;
      let belowHeight =
        belowCount > 0 ? belowCount * view.lineHeight : 0;

      let spacerTop = `<div style="height:${aboveHeight}px"></div>`;
      let spacerBottom = `<div style="height:${belowHeight}px"></div>`;

      // Only the fetched (visible) lines are joined and inserted
      let htmlContent =
        spacerTop + fetchedLines.join('<br>') + spacerBottom;
      content.update(htmlContent);

      if (scrollToBottom) {
        let scroller = view.getScrollable();
        scroller.suspendEvent('scroll');
        view.scrollTo(0, Infinity);
        me.updateStart(true);
        scroller.resumeEvent('scroll');
      }
    },

    doLoad: function () {
      let me = this;
      if (me.running) {
        me.requested = true;
        return;
      }
      me.running = true;
      let view = me.getView();
      let viewModel = me.getViewModel();
      Proxmox.Utils.API2Request({
        url: view.url,
        params: viewModel.get('params'),
        method: 'GET',
        success: function (response) {
          if (me.isDestroyed) {
            return;
          }
          Proxmox.Utils.setErrorMask(me, false);
          let total = response.result.total;
          let fetchedLines = [];
          // Use the 'start' parameter (or 0 if undefined)
          let startParam = viewModel.get('params.start') || 0;
          // Instead of a giant array with empty gaps, we simply push
          // the fetched lines.
          Ext.Array.each(response.result.data, function (line) {
            fetchedLines.push(Ext.htmlEncode(line.t));
          });
          me.updateView(fetchedLines, startParam, total);
          me.running = false;
          if (me.requested) {
            me.requested = false;
            view.loadTask.delay(200);
          }
        },
        failure: function (response) {
          if (view.failCallback) {
            view.failCallback(response);
          } else {
            let msg = response.htmlStatus;
            Proxmox.Utils.setErrorMask(me, msg);
          }
          me.running = false;
          if (me.requested) {
            me.requested = false;
            view.loadTask.delay(200);
          }
        },
      });
    },

    updateStart: function (scrolledToBottom, targetLine) {
      let me = this;
      let view = me.getView(),
        viewModel = me.getViewModel();

      let limit = viewModel.get('params.limit');
      let total = viewModel.get('data').total || 0;

      // Heuristic: if scrolling up load more before, if scrolling down load more after.
      let startRatio =
        view.lastTargetLine && view.lastTargetLine > targetLine
          ? 2 / 3
          : 1 / 3;
      view.lastTargetLine = targetLine;

      let newStart = scrolledToBottom
        ? Math.trunc(total - limit)
        : Math.trunc(targetLine - startRatio * limit + 10);

      viewModel.set('params.start', Math.max(newStart, 0));

      view.loadTask.delay(200);
    },

    onScroll: function (x, y) {
      let me = this;
      let view = me.getView(),
        viewModel = me.getViewModel();

      let line = view.getScrollY() / view.lineHeight;
      let viewLines = view.getHeight() / view.lineHeight;

      let viewStart = Math.max(
        Math.trunc(line - 1 - view.viewBuffer),
        0
      );
      let viewEnd = Math.trunc(line + viewLines + 1 + view.viewBuffer);

      let { start, limit } = viewModel.get('params');
      let margin = start < 20 ? 0 : 20;

      if (viewStart < start + margin || viewEnd > start + limit - margin) {
        me.updateStart(false, line);
      }
    },

    onLiveMode: function () {
      let me = this;
      let viewModel = me.getViewModel();
      viewModel.set('livemode', true);
      viewModel.set('params', { start: 0, limit: 510 });
      let view = me.getView();
      delete view.content;
      view.scrollToEnd = true;
      me.updateView([], 0, 0);
    },

    onTimespan: function () {
      let me = this;
      me.getViewModel().set('livemode', false);
      me.updateView([], 0, 0);
      // Directly apply currently selected values without button click.
      me.updateParams();
    },

    init: function (view) {
      let me = this;

      if (!view.url) {
        throw 'no url specified';
      }

      let viewModel = this.getViewModel();
      let since = new Date();
      since.setDate(since.getDate() - 3);
      viewModel.set('until', new Date());
      viewModel.set('since', since);
      viewModel.set('params.limit', view.pageSize);
      viewModel.set('hide_timespan', !view.log_select_timespan);
      viewModel.set('submitFormat', view.submitFormat);
      me.lookup('content').setStyle(
        'line-height',
        `${view.lineHeight}px`
      );

      view.loadTask = new Ext.util.DelayedTask(me.doLoad, me);

      me.updateParams();
      view.task = Ext.TaskManager.start({
        run: () => {
          if (!view.isVisible() || !view.scrollToEnd) {
            return;
          }
          if (me.scrollPosBottom() <= 5) {
            view.loadTask.delay(200);
          }
        },
        interval: 1000,
      });
    },
  },

  onDestroy: function () {
    let me = this;
    me.loadTask.cancel();
    Ext.TaskManager.stop(me.task);
  },

  // Allow external callers to trigger a load.
  requestUpdate: function () {
    let me = this;
    me.loadTask.delay(200);
  },

  viewModel: {
    data: {
      until: null,
      since: null,
      submitFormat: 'Y-m-d',
      livemode: true,
      hide_timespan: false,
      data: {
        start: 0,
        total: 0,
        textlen: 0,
      },
      params: {
        start: 0,
        limit: 510,
      },
    },
  },

  layout: 'auto',
  bodyPadding: 5,
  scrollable: {
    x: 'auto',
    y: 'auto',
    listeners: {
      // We hook the internal scroller’s scroll event here.
      scroll: {
        fn: function (scroller, x, y) {
          let controller = this.component.getController();
          if (controller) {
            controller.onScroll(x, y);
          }
        },
        buffer: 200,
      },
    },
  },

  tbar: {
    bind: {
      hidden: '{hide_timespan}',
    },
    items: [
      '->',
      {
        xtype: 'segmentedbutton',
        items: [
          {
            text: gettext('Live Mode'),
            bind: {
              pressed: '{livemode}',
            },
            handler: 'onLiveMode',
          },
          {
            text: gettext('Select Timespan'),
            bind: {
              pressed: '{!livemode}',
            },
            handler: 'onTimespan',
          },
        ],
      },
      {
        xtype: 'box',
        autoEl: { cn: gettext('Since') + ':' },
        bind: {
          disabled: '{livemode}',
        },
      },
      {
        xtype: 'proxmoxDateTimeField',
        name: 'since_date',
        reference: 'since',
        format: 'Y-m-d',
        bind: {
          disabled: '{livemode}',
          value: '{since}',
          maxValue: '{until}',
          submitFormat: '{submitFormat}',
        },
      },
      {
        xtype: 'box',
        autoEl: { cn: gettext('Until') + ':' },
        bind: {
          disabled: '{livemode}',
        },
      },
      {
        xtype: 'proxmoxDateTimeField',
        name: 'until_date',
        reference: 'until',
        format: 'Y-m-d',
        bind: {
          disabled: '{livemode}',
          value: '{until}',
          minValue: '{since}',
          submitFormat: '{submitFormat}',
        },
      },
      {
        xtype: 'button',
        text: 'Update',
        handler: 'updateParams',
        bind: {
          disabled: '{livemode}',
        },
      },
    ],
  },

  items: [
    {
      xtype: 'box',
      reference: 'content',
      style: {
        font: 'normal 11px tahoma, arial, verdana, sans-serif',
        'white-space': 'pre',
      },
    },
  ],
});
