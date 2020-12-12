import _ from 'the-lodash';
import { MetaStore } from './meta-store';
import { MetaTableColumn } from './meta-table-column';

export class MetaTable
{
    private _parent : MetaStore;
    private _name : string;
    private _columns : Record<string, MetaTableColumn> = {};

    private _keyFields : string[] = [];
    private _queryFields : string[] = [];
    private _createFields : string[] = [];
    private _deleteFields : string[] = [];


    constructor(parent: MetaStore, name: string)
    {
        this._parent = parent;
        this._name = name;
    }

    private _makeColumn(name: string)
    {
        if (!this._columns[name]) {
            this._columns[name] = new MetaTableColumn(this, name);
        }
        return this._columns[name];
    }

    getName() {
        return this._name;
    }

    getKeyFields() {
        return this._keyFields;
    }

    getQueryFields() {
        return this._queryFields;
    }

    getCreateFields() {
        return this._createFields;
    }

    getUpdateFields() {
        return this._createFields;
    }

    getDeleteFields() {
        return this._deleteFields;
    }

    getColumn(name: string)
    {
        var column = this._columns[name];
        if (!column) {
            throw new Error("Unknown Column: " + name);;
        }
        return column;
    }

    getMassageableColumns()
    {
        return _.values(this._columns).filter(x => x.hasFromDbCb);
    }

    table(name: string)
    {
        return this._parent.table(name);
    }

    key(name: string)
    {
        var column = this._makeColumn(name);
        column.isKey = true;
        column.isSettable = false;

        this._keyFields = 
            _.values(this._columns)
                .filter(x => x.isKey)
                .map(x => x.name);

        this._buildQueryFields();
        this._buildModifyFields();
        return column;
    }

    field(name: string)
    {
        var column = this._makeColumn(name);

        this._buildQueryFields();
        this._buildCreateFields();
        return column;
    }

    private _buildQueryFields()
    {
        this._queryFields = _.concat(
            _.values(this._columns).filter(x => x.isKey),
            _.values(this._columns).filter(x => !x.isKey)
            )
            .map(x => x.name);
    }

    private _buildCreateFields()
    {
        this._createFields = _.concat(
            _.values(this._columns).filter(x => x.isSettable)
            )
            .map(x => x.name);
    }

    private _buildModifyFields()
    {
        this._deleteFields = _.concat(
            _.values(this._columns).filter(x => x.isKey)
            )
            .map(x => x.name);
    }
}
