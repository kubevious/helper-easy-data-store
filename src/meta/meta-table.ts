import _ from 'the-lodash';
import { MetaTableColumn, MetaTableColumnBuilder, MetaTableColumnData } from './meta-table-column';
import { MetaStore, MetaStoreBuilder } from './meta-store';

const SUPPORTED_DRIVERS: Record<string, boolean> = { mysql: true };

export class MetaTableData {
    public name: string = '';
    public driverParams: any;

    public columns: MetaTableColumn[] = [];

    public columnsDict: Record<string, MetaTableColumn> = {};

    public keyFields: string[] = [];
    public keyColumns: MetaTableColumn[] = [];

    public nonKeyFields: string[] = [];
    public nonKeyColumns: MetaTableColumn[] = [];

    public modifyFields: string[] = [];
}

export class MetaTable {
    private _parent: MetaStore;
    private _data: MetaTableData;

    constructor(parent: MetaStore, data: MetaTableData) {
        this._parent = parent;
        this._data = data;
    }

    get name(): string {
        return this._data.name;
    }

    get driverName() {
        return 'mysql';
    }

    get driverParams(): any {
        return this._data.driverParams;
    }

    get keyFields(): string[] {
        return this._data.keyFields;
    }

    get keyColumns(): MetaTableColumn[] {
        return this._data.keyColumns;
    }

    get nonKeyFields(): string[] {
        return this._data.nonKeyFields;
    }

    get nonKeyColumns(): MetaTableColumn[] {
        return this._data.nonKeyColumns;
    }

    get modifyFields(): string[] {
        return this._data.modifyFields;
    }

    get columns(): MetaTableColumn[] {
        return this._data.columns;
    }

    getColumn(name: string): MetaTableColumn {
        const column = this._data.columnsDict[name];
        if (!column) {
            throw new Error(`Unknown Column: ${name}`);
        }
        return column;
    }
}

export class MetaTableBuilder {
    private parent: MetaStoreBuilder;
    private data: MetaTableData;
    private _table: MetaTable;
    private columns: Record<string, MetaTableColumnBuilder> = {};

    constructor(parent: MetaStoreBuilder, data: MetaTableData, table: MetaTable) {
        this.parent = parent;
        this.data = data;
        this._table = table;
    }

    // Self Builder
    driverParams(params: any) {
        this.data.driverParams = params;
        return this;
    }

    key(name: string): MetaTableColumnBuilder {
        const data = new MetaTableColumnData();
        const column = this._makeColumn(data, name);
        data.isKey = true;
        data.isSettable = false;
        this._buildFields();
        return column;
    }

    field(name: string): MetaTableColumnBuilder {
        const data = new MetaTableColumnData();
        const column = this._makeColumn(data, name);
        this._buildFields();
        return column;
    }

    private _makeColumn(data: MetaTableColumnData, name: string): MetaTableColumnBuilder {
        data.name = name;
        const column = new MetaTableColumn(this._table, data);
        const columnBuilder = new MetaTableColumnBuilder(this, data);
        this.columns[name] = columnBuilder;
        this.data.columnsDict[name] = column;
        return columnBuilder;
    }

    private _buildFields() {
        this.data.columns = sortColumns(_.values(this.data.columnsDict));

        this.data.keyFields = this._makeFields((x) => x.isKey);
        this.data.keyColumns = sortColumns(this.data.keyFields.map((x) => this.data.columnsDict[x]));

        this.data.nonKeyFields = this._makeFields((x) => !x.isKey);
        this.data.nonKeyColumns = sortColumns(
            this.data.nonKeyFields.map((x) => this.data.columnsDict[x]),
        );

        this.data.modifyFields = _.concat(
            this.data.columns.filter((x) => x.isKey),
        ).map((x) => x.name);
    }

    private _makeFields(filter: (value: MetaTableColumn) => boolean): string[] {
        return this.data.columns
            .filter((x) => filter(x))
            .map((x) => x.name)
            .sort();
    }

    // Parent Builder

    table(name: string): MetaTableBuilder {
        return this.parent.table(name);
    }
}

function sortColumns(input: MetaTableColumn[]): MetaTableColumn[] {
    return _.sortBy(input, (x) => x.name);
}
