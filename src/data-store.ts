import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { MySqlDriver } from '@kubevious/helper-mysql';
import { DataStoreTable } from './data-store-table';
import { MetaStore } from './meta/meta-store';

export class DataStore
{
    private _meta : MetaStore;
    private _logger : ILogger;
    private _isDebug : boolean = false;
    
    private _mysqlDriver : MySqlDriver;

    constructor(logger : ILogger, isDebug?: boolean, params? : any)
    {
        this._meta = new MetaStore();
        this._logger = logger;
        if (isDebug) {
            this._isDebug = true;
        }

        this._mysqlDriver = new MySqlDriver(this._logger, params, this._isDebug);
    }

    get logger() {
        return this._logger;
    }

    get mysql() {
        return this._mysqlDriver;
    }

    get isConnected() {
        return this.mysql.isConnected;
    }

    meta() {
        return this._meta;
    }

    connect()
    {
        this._mysqlDriver.connect();
    }

    waitConnect()
    {
        return Promise.all([
            this._mysqlDriver.waitConnect()
        ])
    }

    close()
    {
        this._mysqlDriver.close();
    }

    table(name: string) : DataStoreTable
    {
        var metaTable = this._meta.getTable(name);
        return new DataStoreTable(this, metaTable);
    }

    // scope(scope)
    // {
    //     return {
    //         table: (name) => {
    //             var metaTable = this._meta.getTable(name);
    //             return new DataStoreTable(this, metaTable, scope);
    //         }
    //     }
    // }

}