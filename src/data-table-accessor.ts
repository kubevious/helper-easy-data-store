import { DataStore, ITableAccessor } from './data-store'
import { Data, ITableDriver } from './driver'
import { MetaTableBuilder } from './meta/meta-table';

export class DataStoreTableAccessor<TRow>
{
    private _dataStore: ITableAccessor;
    private _tableName: string;

    constructor(dataStore: ITableAccessor, tableName: string)
    {
        this._dataStore = dataStore;
        this._tableName = tableName;
    }

    get tableName() {
        return this._tableName;
    }

    table() : ITableDriver<TRow>
    {
        return this._dataStore.table<TRow>(this._tableName);
    }
}

export class DataStoreAccessorMetaBuilder<TRow>
{
    private _tableName: string;
    private _cb : (tableBuilder : MetaTableBuilder) => void;

    constructor(tableName: string, cb: (tableBuilder : MetaTableBuilder) => void)
    {
        this._tableName = tableName;
        this._cb = cb;
    }

    prepare(dataStore: DataStore) : DataStoreTableAccessor<TRow>
    {
        const metaBuilder = dataStore.meta().table(this._tableName);
        this._cb(metaBuilder);
        return new DataStoreTableAccessor<TRow>(dataStore, this._tableName);
    }
}

export function BuildTableMeta<TRow>(tableName: string, cb: (tableBuilder : MetaTableBuilder) => void) : DataStoreAccessorMetaBuilder<TRow>
{
    const builder = new DataStoreAccessorMetaBuilder<TRow>(tableName, cb);
    return builder;
}