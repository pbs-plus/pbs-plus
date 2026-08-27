package js

import "maps"

// XType is an ExtJS widget alias. Constants cover every xtype in use today.
type XType string

const (
	XProxmoxButton     XType = "proxmoxButton"
	XStdRemoveButton   XType = "proxmoxStdRemoveButton"
	XButton            XType = "button"
	XTbText            XType = "tbtext"
	XTbSeparator       XType = "tbseparator"
	XTextField         XType = "textfield"
	XProxmoxTextField  XType = "proxmoxtextfield"
	XTextArea          XType = "textarea"
	XNumberField       XType = "numberfield"
	XIntegerField      XType = "proxmoxintegerfield"
	XCheckbox          XType = "proxmoxcheckbox"
	XPlainCheckbox     XType = "checkbox"
	XCheckboxGroup     XType = "checkboxgroup"
	XRadioField        XType = "radiofield"
	XRadioGroup        XType = "radiogroup"
	XComboBox          XType = "combobox"
	XCombo             XType = "combo"
	XKVComboBox        XType = "proxmoxKVComboBox"
	XDisplayField      XType = "displayfield"
	XDisplayEditField  XType = "pmxDisplayEditField"
	XHiddenField       XType = "hiddenfield"
	XHidden            XType = "hidden"
	XDateField         XType = "datefield"
	XTimeField         XType = "timefield"
	XFieldContainer    XType = "fieldcontainer"
	XFieldSet          XType = "fieldset"
	XInputPanel        XType = "inputpanel"
	XPanel             XType = "panel"
	XContainer         XType = "container"
	XComponent         XType = "component"
	XForm              XType = "form"
	XGrid              XType = "grid"
	XTabPanel          XType = "tabpanel"
	XBox               XType = "box"
	XLoadMask          XType = "loadmask"
	XDateColumn        XType = "datecolumn"
	XNumberColumn      XType = "numbercolumn"
	XTreeColumn        XType = "treecolumn"
	XActionColumn      XType = "actioncolumn"
	XRowNumberer       XType = "rownumberer"
	XUserSelector      XType = "pmxUserSelector"
	XDataStoreSelector XType = "pbsDataStoreSelector"
)

// Component is a typed ExtJS construct that lowers to a config object.
type Component interface {
	Config() Obj
}

func set(o Obj, k string, v any) {
	switch t := v.(type) {
	case nil:
	case string:
		if t != "" {
			o[k] = t
		}
	case XType:
		if t != "" {
			o[k] = string(t)
		}
	case Raw:
		if t != "" {
			o[k] = t
		}
	case int:
		if t != 0 {
			o[k] = t
		}
	case bool:
		if t {
			o[k] = true
		}
	case *bool:
		if t != nil {
			o[k] = *t
		}
	case Obj:
		if len(t) > 0 {
			o[k] = t
		}
	case Arr:
		if len(t) > 0 {
			o[k] = t
		}
	default:
		o[k] = v
	}
}

// Bool returns a pointer for tri-state config fields, where an explicit false
// differs from an unset field.
//
//go:fix inline
func Bool(b bool) *bool { return new(b) }

// Items lowers a mixed list of Components and raw values to an array.
func Items(items ...any) Arr {
	out := make(Arr, 0, len(items))
	for _, it := range items {
		if c, ok := it.(Component); ok {
			out = append(out, c.Config())
			continue
		}
		out = append(out, it)
	}
	return out
}

// PlusURL builds a PBS-Plus API URL expression rooted at the pbsPlusBaseUrl
// global from pre/1_initialization.js.
func PlusURL(path string) Raw {
	return Raw("pbsPlusBaseUrl + " + string(quote(path)))
}

// Call renders a call expression; pass Raw for identifier arguments.
func Call(fn string, args ...any) Raw {
	dst := append([]byte(fn), '(')
	for i, a := range args {
		if i > 0 {
			dst = append(dst, ", "...)
		}
		dst = appendValue(dst, a, 0)
	}
	return Raw(append(dst, ')'))
}

// New renders an Ext.create call.
func New(class string, config Obj) Raw { return Call("Ext.create", class, config) }

// API2Request renders a PBS.PlusUtils.API2Request call.
func API2Request(config Obj) Raw { return Call("PBS.PlusUtils.API2Request", config) }

// ModelField is a store field. Type is empty for plain string fields.
type ModelField struct {
	Name    string
	Type    string
	Convert Raw
}

// Fields builds untyped model fields from their names.
func Fields(names ...string) []ModelField {
	out := make([]ModelField, len(names))
	for i, n := range names {
		out[i] = ModelField{Name: n}
	}
	return out
}

// Typed appends a typed field, such as {"name": "stale", "type": "bool"}.
func Typed(fields []ModelField, name, typ string) []ModelField {
	return append(fields, ModelField{Name: name, Type: typ})
}

// Model is an Ext.data.Model class backed by a PBS-Plus API path.
type Model struct {
	Name         string
	Extend       string
	Fields       []ModelField
	IDProperty   string
	APIPath      string
	RootProperty string
}

func (m Model) Config() Obj {
	fields := make(Arr, len(m.Fields))
	for i, f := range m.Fields {
		if f.Type == "" && f.Convert == "" {
			fields[i] = f.Name
			continue
		}
		field := Obj{"name": f.Name, "type": f.Type}
		set(field, "convert", f.Convert)
		fields[i] = field
	}
	extend := m.Extend
	if extend == "" {
		extend = "Ext.data.Model"
	}
	o := Obj{"extend": extend, "fields": fields}
	set(o, "idProperty", m.IDProperty)
	if m.APIPath != "" {
		root := m.RootProperty
		if root == "" {
			root = "data"
		}
		o["proxy"] = Obj{
			"type":   "pbsplus",
			"url":    PlusURL(m.APIPath),
			"reader": Obj{"type": "json", "rootProperty": root},
		}
	}
	return o
}

func (m Model) AppendJS(dst []byte, indent int) []byte {
	return Class{Name: m.Name, Config: m.Config()}.AppendJS(dst, indent)
}

// Store is the diff-over-update store every PBS-Plus grid uses.
type Store struct {
	StoreID        string
	Model          string
	Fields         []string
	Data           []Option
	APIPath        string
	Sorters        string
	SortBy         []Sorter
	GroupField     string
	Proxy          StoreProxy
	QueryParamNull bool
	Interval       int
}

type Sorter struct {
	Property  string
	Direction string
}

func (s Sorter) Config() Obj {
	return Obj{"property": s.Property, "direction": s.Direction}
}

func sorters(list []Sorter) Arr {
	out := make(Arr, len(list))
	for i, s := range list {
		out[i] = s.Config()
	}
	return out
}

type StoreProxy string

const (
	ProxyPBSPlus StoreProxy = "pbsplus"
	ProxyProxmox StoreProxy = "proxmox"
)

func (s Store) Config() Obj {
	proxyType := s.Proxy
	if proxyType == "" {
		proxyType = ProxyPBSPlus
	}
	url := any(PlusURL(s.APIPath))
	if proxyType == ProxyProxmox {
		url = s.APIPath
	}
	proxy := Obj{"type": string(proxyType), "url": url}
	if s.QueryParamNull {
		proxy["queryParam"] = Raw("null")
	}
	if len(s.Fields) > 0 {
		plain := Obj{"fields": stringsToArr(s.Fields), "autoLoad": true, "proxy": proxy}
		set(plain, "sorters", s.Sorters)
		return plain
	}
	if len(s.Data) > 0 {
		data := make(Arr, len(s.Data))
		for i, option := range s.Data {
			data[i] = option.Config()
		}
		return Obj{"fields": Arr{"value", "text"}, "data": data}
	}
	if s.Model == "" {
		plain := Obj{"proxy": proxy, "autoLoad": true}
		set(plain, "sorters", s.Sorters)
		return plain
	}
	rstore := Obj{"type": "update", "storeid": s.StoreID, "model": s.Model}
	if s.APIPath != "" {
		rstore["proxy"] = proxy
	}
	set(rstore, "interval", s.Interval)
	o := Obj{"type": "diff", "rstore": rstore}
	set(o, "sorters", s.Sorters)
	if len(s.SortBy) > 0 {
		o["sorters"] = sorters(s.SortBy)
	}
	set(o, "groupField", s.GroupField)
	return o
}

// Column is a grid column.
type Column struct {
	Text           string
	DataIndex      string
	XType          XType
	Flex           float64
	Width          int
	Hidden         bool
	Align          string
	Sortable       *bool
	Renderer       Raw
	RendererMethod string
}

func (c Column) Config() Obj {
	o := Obj{}
	if c.Text != "" {
		o["text"] = T(c.Text)
	}
	set(o, "dataIndex", c.DataIndex)
	set(o, "xtype", c.XType)
	set(o, "flex", c.Flex)
	set(o, "width", c.Width)
	set(o, "hidden", c.Hidden)
	set(o, "align", c.Align)
	set(o, "sortable", c.Sortable)
	set(o, "renderer", c.Renderer)
	set(o, "renderer", c.RendererMethod)
	return o
}

// Tool is a toolbar entry. Handler names a controller method; HandlerFn holds
// an inline function instead.
type Tool struct {
	Text                  string
	XType                 XType
	Handler               string
	HandlerFn             Raw
	Disabled              bool
	SelModel              *bool
	EnableFn              Raw
	IconCls               string
	StandardRemoveBaseURL string
	Callback              string
	ItemID                string
	Hidden                bool
	Cls                   string
	HTML                  string

	separator string
}

// Sep is the toolbar separator entry.
func Sep() Tool { return Tool{separator: "-"} }

// Fill pushes the remaining toolbar entries to the far end.
func Fill() Tool { return Tool{separator: "->"} }

func (t Tool) Config() Obj {
	xtype := string(XProxmoxButton)
	if t.StandardRemoveBaseURL != "" {
		xtype = "proxmoxStdRemoveButton"
	}
	o := Obj{"xtype": xtype}
	set(o, "xtype", t.XType)
	if t.Text != "" {
		o["text"] = T(t.Text)
	}
	set(o, "handler", t.Handler)
	set(o, "handler", t.HandlerFn)
	set(o, "disabled", t.Disabled)
	set(o, "selModel", t.SelModel)
	set(o, "enableFn", t.EnableFn)
	set(o, "iconCls", t.IconCls)
	set(o, "baseurl", t.StandardRemoveBaseURL)
	set(o, "callback", t.Callback)
	set(o, "itemId", t.ItemID)
	set(o, "hidden", t.Hidden)
	set(o, "cls", t.Cls)
	set(o, "html", t.HTML)
	return o
}

func tools(list []Tool) Arr {
	out := make(Arr, len(list))
	for i, t := range list {
		if t.separator != "" {
			out[i] = t.separator
			continue
		}
		out[i] = t.Config()
	}
	return out
}

// Controller is an inline Ext.app.ViewController. Methods maps a name to a
// function literal, normally built with Func.
type Controller struct {
	Methods map[string]Raw
}

func (c Controller) Config() Obj {
	o := Obj{"xclass": "Ext.app.ViewController"}
	for k, v := range c.Methods {
		o[k] = v
	}
	return o
}

// Listeners maps component events to controller method names.
type Listeners struct {
	Activate        string
	Deactivate      string
	BeforeDestroy   string
	ItemDblClick    string
	AfterRender     string
	SelectionChange string
}

func (l Listeners) Config() Obj {
	o := Obj{}
	set(o, "activate", l.Activate)
	set(o, "deactivate", l.Deactivate)
	set(o, "beforedestroy", l.BeforeDestroy)
	set(o, "itemdblclick", l.ItemDblClick)
	set(o, "afterrender", l.AfterRender)
	set(o, "selectionchange", l.SelectionChange)
	return o
}

type ViewConfig struct {
	GetRowClass    Raw
	TrackOver      *bool
	StripeRows     *bool
	DeferEmptyText *bool
}

func (v ViewConfig) Config() Obj {
	o := Obj{}
	set(o, "getRowClass", v.GetRowClass)
	set(o, "trackOver", v.TrackOver)
	set(o, "stripeRows", v.StripeRows)
	set(o, "deferEmptyText", v.DeferEmptyText)
	return o
}

type Grouping struct {
	HeaderTemplate string
	FormatName     Raw
}

func (g Grouping) Config() Obj {
	return Obj{"ftype": "grouping", "groupHeaderTpl": Arr{g.HeaderTemplate, Obj{"formatName": g.FormatName}}}
}

func withStoreLifecycle(c Controller) Controller {
	m := map[string]Raw{
		"reload":     Func("", "this.getView().getStore().rstore.load();"),
		"stopStore":  Func("", "this.getView().getStore().rstore.stopUpdate();"),
		"startStore": Func("", "this.getView().getStore().rstore.startUpdate();"),
		"init":       Func("view", "Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);"),
	}
	maps.Copy(m, c.Methods)
	c.Methods = m
	return c
}

func withReload(c Controller) Controller {
	m := map[string]Raw{"reload": Func("", "this.getView().getStore().load();")}
	maps.Copy(m, c.Methods)
	c.Methods = m
	return c
}

func withStoreListeners(l Listeners) Listeners {
	if l.BeforeDestroy == "" {
		l.BeforeDestroy = "stopStore"
	}
	if l.Deactivate == "" {
		l.Deactivate = "stopStore"
	}
	if l.Activate == "" {
		l.Activate = "startStore"
	}
	return l
}

// Field is a form field or generic child component.
type Field struct {
	XType                    XType
	Name                     string
	ItemID                   string
	Label                    string
	Value                    any
	AllowBlank               *bool
	Editable                 *bool
	EditableWhenCreate       bool
	DeleteEmptyWhenNotCreate bool
	Disabled                 bool
	Renderer                 Raw
	Width                    int
	Height                   int
	Layout                   string
	Anchor                   string
	HTML                     string
	EmptyText                string
	InputType                string
	UserCls                  string
	Title                    string
	IconCls                  string
	Items                    Arr
	AfterRender              Raw
}

func (f Field) Config() Obj {
	o := Obj{}
	set(o, "xtype", f.XType)
	set(o, "name", f.Name)
	set(o, "itemId", f.ItemID)
	if f.Label != "" {
		o["fieldLabel"] = T(f.Label)
	}
	if f.Value != nil {
		o["value"] = f.Value
	}
	set(o, "allowBlank", f.AllowBlank)
	set(o, "editable", f.Editable)
	if f.EditableWhenCreate || f.DeleteEmptyWhenNotCreate {
		cbind := Obj{}
		if f.EditableWhenCreate {
			cbind["editable"] = "{isCreate}"
		}
		if f.DeleteEmptyWhenNotCreate {
			cbind["deleteEmpty"] = "{!isCreate}"
		}
		o["cbind"] = cbind
	}
	set(o, "disabled", f.Disabled)
	set(o, "renderer", f.Renderer)
	set(o, "width", f.Width)
	set(o, "height", f.Height)
	set(o, "layout", f.Layout)
	set(o, "anchor", f.Anchor)
	set(o, "html", f.HTML)
	if f.EmptyText != "" {
		o["emptyText"] = T(f.EmptyText)
	}
	set(o, "inputType", f.InputType)
	set(o, "userCls", f.UserCls)
	if f.Title != "" {
		o["title"] = T(f.Title)
	}
	set(o, "iconCls", f.IconCls)
	set(o, "items", f.Items)
	set(o, "listeners", listener("afterrender", f.AfterRender))
	return o
}

// EditWindow is a PBS.plusWindow.Edit dialog. CBindData receives the window
// initialConfig and normally sets me.url and me.method.
type EditWindow struct {
	Name         string
	XType        XType
	Extend       string
	Subject      string
	Width        string
	PixelWidth   int
	Resizable    bool
	NotResizable bool
	IsCreate     bool
	IsAdd        bool
	Method       string
	CBindData    Raw
	Items        Arr
	Methods      map[string]Raw
}

func (w EditWindow) Config() Obj {
	extend := w.Extend
	if extend == "" {
		extend = "PBS.plusWindow.Edit"
	}
	o := Obj{
		"extend": extend,
		"mixins": Arr{"Proxmox.Mixin.CBind"},
	}
	if w.XType != "" {
		o["alias"] = "widget." + string(w.XType)
	}
	set(o, "subject", w.Subject)
	set(o, "width", w.Width)
	set(o, "width", w.PixelWidth)
	set(o, "resizable", w.Resizable)
	if w.NotResizable {
		o["resizable"] = false
	}
	set(o, "isCreate", w.IsCreate)
	set(o, "isAdd", w.IsAdd)
	set(o, "method", w.Method)
	set(o, "cbindData", w.CBindData)
	set(o, "items", w.Items)
	for k, v := range w.Methods {
		o[k] = v
	}
	return o
}

func (w EditWindow) AppendJS(dst []byte, indent int) []byte {
	return Class{Name: w.Name, Config: w.Config()}.AppendJS(dst, indent)
}

// Selector is a Proxmox.form.ComboGrid bound to a PBS-Plus API path.
type Selector struct {
	Name            string
	XType           XType
	Extend          string
	DisplayField    string
	ValueField      string
	APIPath         string
	Sorters         string
	AllowBlank      *bool
	AutoSelect      *bool
	ListWidth       int
	ListColumns     []Column
	Value           Raw
	Editable        *bool
	ForceSelection  *bool
	QueryMode       string
	MinChars        int
	FilterPickList  *bool
	TypeAhead       *bool
	AnyMatch        *bool
	MatchFieldWidth *bool
	ConfigNames     []string
	DeleteEmpty     *bool
	EmptyText       string
	ListMinWidth    int
	ListMaxWidth    int
	ListMinHeight   int
	ListEmptyText   Raw
	Clearable       bool
	AfterRender     Raw
	OnChange        Raw
	SubmitData      Raw
	Options         []Option
	Template        []string
	DisplayTemplate []string
	Methods         map[string]Raw
}

func (s Selector) Config() Obj {
	extend := s.Extend
	if extend == "" {
		extend = "Proxmox.form.ComboGrid"
	}
	o := Obj{"extend": extend}
	if s.XType != "" {
		o["alias"] = "widget." + string(s.XType)
	}
	set(o, "displayField", s.DisplayField)
	set(o, "valueField", s.ValueField)
	set(o, "allowBlank", s.AllowBlank)
	set(o, "autoSelect", s.AutoSelect)
	set(o, "value", s.Value)
	set(o, "editable", s.Editable)
	set(o, "forceSelection", s.ForceSelection)
	set(o, "queryMode", s.QueryMode)
	set(o, "minChars", s.MinChars)
	set(o, "filterPickList", s.FilterPickList)
	set(o, "typeAhead", s.TypeAhead)
	set(o, "anyMatch", s.AnyMatch)
	set(o, "matchFieldWidth", s.MatchFieldWidth)
	if len(s.ConfigNames) > 0 || s.DeleteEmpty != nil {
		config := Obj{}
		for _, name := range s.ConfigNames {
			config[name] = Raw("null")
		}
		set(config, "deleteEmpty", s.DeleteEmpty)
		o["config"] = config
	}
	set(o, "emptyText", s.EmptyText)
	if s.APIPath != "" {
		o["store"] = Store{APIPath: s.APIPath, Sorters: s.Sorters}.Config()
	}
	if len(s.Options) > 0 {
		o["store"] = Store{Data: s.Options}.Config()
	}
	if len(s.ListColumns) > 0 || s.ListWidth != 0 || s.ListMinWidth != 0 || s.ListMaxWidth != 0 || s.ListMinHeight != 0 || s.ListEmptyText != "" {
		cols := make(Arr, len(s.ListColumns))
		for i, c := range s.ListColumns {
			cols[i] = c.Config()
		}
		list := Obj{"columns": cols}
		set(list, "width", s.ListWidth)
		set(list, "minWidth", s.ListMinWidth)
		set(list, "maxWidth", s.ListMaxWidth)
		set(list, "minHeight", s.ListMinHeight)
		set(list, "emptyText", s.ListEmptyText)
		o["listConfig"] = list
	}
	if s.Clearable {
		o["triggers"] = Obj{"clear": Obj{"cls": "pmx-clear-trigger", "weight": -1, "hidden": true, "handler": Func("", `this.triggers.clear.setVisible(false); this.setValue("");`)}}
	}
	listeners := listener("change", s.OnChange)
	if s.AfterRender != "" {
		if listeners == nil {
			listeners = Obj{}
		}
		listeners["afterrender"] = s.AfterRender
	}
	set(o, "listeners", listeners)
	set(o, "getSubmitData", s.SubmitData)
	if len(s.Template) > 0 {
		o["tpl"] = stringsToArr(s.Template)
	}
	if len(s.DisplayTemplate) > 0 {
		o["displayTpl"] = stringsToArr(s.DisplayTemplate)
	}
	for k, v := range s.Methods {
		o[k] = v
	}
	return o
}

// Option is a labeled value in a local ExtJS store.
type Option struct {
	Value string
	Text  string
}

func (o Option) Config() Obj {
	return Obj{"value": o.Value, "text": T(o.Text)}
}

func stringsToArr(values []string) Arr {
	result := make(Arr, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}

func listener(event string, handler Raw) Obj {
	if handler == "" {
		return nil
	}
	return Obj{event: handler}
}

func (s Selector) AppendJS(dst []byte, indent int) []byte {
	return Class{Name: s.Name, Config: s.Config()}.AppendJS(dst, indent)
}

// FieldContainer is an Ext.form.FieldContainer class.
// Panel is every container class: grid, field container, input panel, tab
// panel or plain panel. Extend picks the base class, defaulting to a grid when
// Columns are set. A Panel with a Store stops updates on deactivate/destroy,
// restarts on activate and monitors store errors unless NoStoreLifecycle is set.
type Panel struct {
	Name              string
	XType             XType
	Extend            string
	Title             string
	StateID           string
	Layout            string
	Items             Arr
	Column1           Arr
	Column2           Arr
	FieldDefaults     Obj
	Padding           int
	Border            bool
	PanelDefaults     bool
	Store             Component
	Columns           []Column
	Tbar              []Tool
	Grouping          *Grouping
	ViewConfig        *ViewConfig
	Controller        Controller
	Listeners         Listeners
	MultiSelect       bool
	CheckboxSelection bool
	NoStoreLifecycle  bool
	Methods           map[string]Raw
}

const (
	ExtFieldContainer = "Ext.form.FieldContainer"
	ExtInputPanel     = "Proxmox.panel.InputPanel"
	ExtTabPanel       = "Ext.tab.Panel"
)

func (p Panel) Config() Obj {
	extend := p.Extend
	if extend == "" {
		extend = "Ext.panel.Panel"
		if len(p.Columns) > 0 {
			extend = "Ext.grid.Panel"
		}
	}
	o := Obj{"extend": extend}
	if p.XType != "" {
		o["alias"] = "widget." + string(p.XType)
	}
	if p.Title != "" {
		o["title"] = T(p.Title)
	}
	if p.StateID != "" {
		o["stateful"] = true
		o["stateId"] = p.StateID
	}
	set(o, "layout", p.Layout)
	set(o, "items", p.Items)
	set(o, "column1", p.Column1)
	set(o, "column2", p.Column2)
	set(o, "fieldDefaults", p.FieldDefaults)
	set(o, "padding", p.Padding)
	set(o, "border", p.Border)
	if p.PanelDefaults {
		o["defaults"] = Obj{"border": false, "xtype": string(XPanel)}
	}
	set(o, "multiSelect", p.MultiSelect)
	if p.CheckboxSelection {
		o["selType"] = "checkboxmodel"
	}

	ctrl := p.Controller
	listeners := p.Listeners
	if p.Store != nil {
		o["store"] = p.Store.Config()
		if !p.NoStoreLifecycle {
			if s, ok := p.Store.(Store); ok && len(s.Fields) > 0 {
				ctrl = withReload(ctrl)
			} else {
				ctrl = withStoreLifecycle(ctrl)
				listeners = withStoreListeners(listeners)
			}
		}
	}
	if len(ctrl.Methods) > 0 {
		o["controller"] = ctrl.Config()
	}
	if lc := listeners.Config(); len(lc) > 0 {
		o["listeners"] = lc
	}
	if len(p.Columns) > 0 {
		cols := make(Arr, len(p.Columns))
		for i, c := range p.Columns {
			cols[i] = c.Config()
		}
		o["columns"] = cols
	}
	if len(p.Tbar) > 0 {
		o["tbar"] = tools(p.Tbar)
	}
	if p.Grouping != nil {
		o["features"] = Arr{p.Grouping.Config()}
	}
	if p.ViewConfig != nil {
		o["viewConfig"] = p.ViewConfig.Config()
	}
	for k, v := range p.Methods {
		o[k] = v
	}
	return o
}

func (p Panel) AppendJS(dst []byte, indent int) []byte {
	return Class{Name: p.Name, Config: p.Config()}.AppendJS(dst, indent)
}
