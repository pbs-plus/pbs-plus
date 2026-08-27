// Package tape overrides native PBS tape UI classes (post-bundle patching).
package tape

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

// Definitions replaces the flat native tape snapshot grid (slow O(n^2) submit, no grouping).
func Definitions() []js.Value {
	return []js.Value{snapshotGridOverride}
}

var snapshotGridOverride = js.Raw(`
Ext.define('PBS.Plus.TapeSnapshotGridOverride', {
    override: 'PBS.TapeManagement.SnapshotGrid',

    plugins: [],

    features: [
        {
            ftype: 'grouping',
            groupHeaderTpl:
                '<input type="checkbox" class="pbs-tape-group-check" data-group="{[Ext.String.htmlEncode(values.name)]}">' +
                '<span>{[Ext.String.htmlEncode(values.name)]}</span>' +
                '<span style="opacity: 0.6; margin-left: 8px">({rows.length} ' + gettext('Snapshots') + ')</span>',
            hideGroupedHeader: false,
            enableGroupingMenu: false,
            enableNoGroups: false,
            startCollapsed: false,
        },
    ],

    store: {
        groupField: 'group',
        sorters: [
            { property: 'group', direction: 'ASC' },
            { property: 'snapshot', direction: 'ASC' },
        ],
        data: [],
        filters: [],
    },

    tbar: [
        {
            xtype: 'textfield',
            emptyText: gettext('Search'),
            width: 250,
            triggers: {
                clear: {
                    cls: 'pmx-clear-trigger',
                    handler: function (field) {
                        field.setValue('');
                    },
                },
            },
            listeners: {
                change: {
                    buffer: 250,
                    fn: function (field, value) {
                        let grid = field.up('pbsTapeSnapshotGrid');
                        let v = (value || '').toLowerCase();
                        grid.getStore().filter({
                            id: 'pbs-snapshot-search',
                            filterFn: function (rec) {
                                if (!v) {
                                    return true;
                                }
                                return (
                                    rec.get('snapshot').toLowerCase().indexOf(v) !== -1 ||
                                    rec.get('store').toLowerCase().indexOf(v) !== -1 ||
                                    (rec.get('ns') || '').toLowerCase().indexOf(v) !== -1
                                );
                            },
                        });
                    },
                },
            },
        },
        '->',
        {
            text: gettext('Select All'),
            handler: function (btn) {
                btn.up('pbsTapeSnapshotGrid').getSelectionModel().selectAll();
            },
        },
        {
            text: gettext('Deselect All'),
            handler: function (btn) {
                btn.up('pbsTapeSnapshotGrid').getSelectionModel().deselectAll();
            },
        },
    ],

    columns: [
        {
            text: gettext('Snapshot'),
            dataIndex: 'tail',
            flex: 2,
        },
        {
            text: gettext('Namespace'),
            dataIndex: 'ns',
            flex: 1,
            renderer: function (v) {
                return v === '' ? Proxmox.Utils.NoneText : v;
            },
        },
    ],

    viewConfig: {
        emptyText: gettext('No Snapshots'),
        markDirty: false,
        listeners: {
            refresh: function (view) {
                if (view.ownerGrid && view.ownerGrid.syncGroupChecks) {
                    view.ownerGrid.syncGroupChecks();
                }
            },
            groupheaderclick: function (view, node, group, e) {
                if (!e.getTarget('.pbs-tape-group-check')) {
                    return;
                }
                e.stopEvent();
                let grid = view.ownerGrid;
                let store = view.getStore();
                let recs = [];
                store.getData().getRange().forEach(function (rec) {
                    if (rec.get('group') === group) {
                        recs.push(rec);
                    }
                });
                let sm = grid.getSelectionModel();
                let all = recs.every(function (rec) {
                    return sm.isSelected(rec);
                });
                if (all) {
                    sm.deselect(recs);
                } else {
                    sm.select(recs, true);
                }
            },
        },
    },

    getValue: function () {
        let me = this;
        let snapshots = [];
        let selectedStoreCounts = {};

        let data = me.store.getData();
        let visible = new Set();
        (data.items || data.getRange()).forEach(function (rec) {
            visible.add(rec.get('id'));
        });

        me.getSelection().forEach(function (rec) {
            let id = rec.get('id');
            if (!visible.has(id)) {
                return;
            }
            let store = rec.get('store');
            snapshots.push(store + ':' + rec.get('snapshot'));
            if (selectedStoreCounts[store] === undefined) {
                selectedStoreCounts[store] = 0;
            }
            selectedStoreCounts[store]++;
        });

        let originalData = data.getSource() || data;
        if (snapshots.length === originalData.length) {
            return [];
        }

        let wholeStores = [];
        let onlyWholeStoresSelected = true;
        for (const [store, count] of Object.entries(selectedStoreCounts)) {
            if (me.storeCounts[store] === count) {
                wholeStores.push(store);
            } else {
                onlyWholeStoresSelected = false;
                break;
            }
        }

        if (onlyWholeStoresSelected) {
            return wholeStores;
        }

        return snapshots;
    },

    setData: function (records) {
        let me = this;
        let storeCounts = {};
        let mapped = records.map(function (rec) {
            let parts = rec.snapshot.split('/');
            let ns = parts.length > 3 ? parts.slice(0, parts.length - 3).join('/') : '';
            let group = ns === '' ? rec.store : rec.store + ':' + ns;
            storeCounts[rec.store] = (storeCounts[rec.store] || 0) + 1;
            return Ext.apply({}, rec, {
                ns: ns,
                tail: parts.slice(-3).join('/'),
                group: group,
            });
        });
        me.storeCounts = storeCounts;
        me.getStore().setData(mapped);
    },

    syncGroupChecks: function () {
        let me = this;
        if (me.isDestroyed || !me.rendered) {
            return;
        }
        let view = me.getView();
        if (!view || !view.el) {
            return;
        }
        let store = me.getStore();
        let sm = me.getSelectionModel();
        let groups = {};
        store.getData().getRange().forEach(function (rec) {
            let g = rec.get('group');
            if (!groups[g]) {
                groups[g] = { total: 0, sel: 0 };
            }
            groups[g].total++;
            if (sm.isSelected(rec)) {
                groups[g].sel++;
            }
        });
        view.el.query('.pbs-tape-group-check').forEach(function (el) {
            let st = groups[el.getAttribute('data-group')];
            if (!st) {
                return;
            }
            el.checked = st.sel > 0 && st.sel === st.total;
            el.indeterminate = st.sel > 0 && st.sel < st.total;
        });
    },

    initComponent: function () {
        let me = this;
        me.callParent(arguments);

        if (me.prefilter !== undefined) {
            if (me.prefilter.store !== undefined) {
                me.store.filters.add({
                    id: 'x-gridfilter-store',
                    property: 'store',
                    operator: 'in',
                    value: [me.prefilter.store],
                });
            }

            if (me.prefilter.snapshot !== undefined) {
                me.store.filters.add({
                    id: 'x-gridfilter-snapshot',
                    property: 'snapshot',
                    value: me.prefilter.snapshot,
                });
            }
        }

        me.mon(me.store, 'filterchange', function () {
            me.syncGroupChecks();
            me.checkChange();
        });
        me.mon(me.getSelectionModel(), 'selectionchange', function () {
            me.syncGroupChecks();
        });
    },
});
`)
