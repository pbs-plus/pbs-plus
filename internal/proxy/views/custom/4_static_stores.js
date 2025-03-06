Ext.define('PBS.PlusModeStore', {
  extend: 'Ext.data.Store',
  model: 'BackupMode',
  data: [
    {'display': 'Metadata', 'value': 'metadata'},
    {'display': 'Data', 'value': 'data'},
    {'display': 'Legacy', 'value': 'legacy'},
  ],
});
