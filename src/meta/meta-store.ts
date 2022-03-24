import _ from 'the-lodash';
import { MetaTable, MetaTableData, MetaTableBuilder } from './meta-table';

export class MetaStoreData {
    public tables: Record<string, MetaTable> = {};
}

export class MetaStore {
    private _data: MetaStoreData;

    constructor(data: MetaStoreData) {
        this._data = data;
    }

    get tables(): MetaTable[] {
        return _.values(this._data.tables);
    }

    getTable(name: string): MetaTable {
        const table = this._data.tables[name];
        if (!table) {
            throw new Error('Unknown table: ' + name);
        }
        return table;
    }
}

export class MetaStoreBuilder {
    private _store: MetaStore;
    private _data: MetaStoreData;
    private _tables: Record<string, MetaTableBuilder> = {};

    constructor(store: MetaStore, data: MetaStoreData) {
        this._store = store;
        this._data = data;
    }

    table(name: string): MetaTableBuilder {
        const tableData = new MetaTableData();
        tableData.name = name;
        const table = new MetaTable(this._store, tableData);
        const builder = new MetaTableBuilder(this, tableData, table);
        this._data.tables[name] = table;
        this._tables[name] = builder;
        return builder;
    }
}
