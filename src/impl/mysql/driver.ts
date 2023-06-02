import _ from 'the-lodash'
import { Data, IDriver, InternalTableDriver } from '../../driver';
import { ILogger } from 'the-logger';
import { MyPromise, Resolvable } from 'the-promise';

import { CacheStore } from '@kubevious/helper-cache';
import { MySqlDriver } from '@kubevious/helper-mysql';
import { MetaTable } from '../../meta/meta-table';
import { MySqlTableImpl } from './table';
import { MySqlTableStatementStore } from './statement-store';
import { TablesData } from '../../table-data';

import { ConnectFunc } from '../../driver';

import dotenv from 'dotenv';
dotenv.config();

export class MySQL implements IDriver {
    private logger: ILogger;
    private _isDebug: boolean = false;
    private _tableStatements: Record<string, MySqlTableStatementStore> = {};
    private _connectListeners : ConnectFunc[] = [];
    private _tables: MetaTable[] = [];
    private _databases: Record<string, MySqlDriver> = {};
    private _tableDrivers: Record<string, MySqlDriver> = {};
    private _isConnected = false;
    private _tablesData : TablesData;

    constructor(logger: ILogger, isDebug: boolean, tables: MetaTable[], tablesData: TablesData)
    {
        this.logger = logger;
        this._tables = tables;
        this._isDebug = isDebug;
        this._tablesData = tablesData;

        for(const table of tables)
        {
            this._setupTableDB(table);
        }
    }

    get databaseClients() {
        return _.keys(this._databases).map(x => ({
            name: x,
            client: this._databases[x]
        }))
    } 

    connect(): Resolvable<any> {
        return this._exec(x => x.connect());
    }

    close(): Resolvable<any> {
        return Promise.resolve()
            .then(() => this._exec(x => x.close()))
            .then(() => {

            })
    }

    executeInTransaction<T>(tableMetas: MetaTable[], cb: () => Resolvable<T>) : Promise<any>
    {
        const driverDict : Record<string, MySqlDriver> = {};
        for(const tableMeta of tableMetas)
        {
            const db = this.getTableDbName(tableMeta);
            const driver = this._databases[db];
            if (!driver) {
                throw new Error("Unknown driver for table " + tableMeta.name);
            }
            driverDict[db] = driver;
        }

        if (_.keys(driverDict).length == 0) {
            throw new Error('No Mysql DB Driver Found.');
        }
        if (_.keys(driverDict).length > 1) {
            throw new Error('Cannot run transactions cross multiple mysql databases');
        }

        const myDriver = driverDict[_.keys(driverDict)[0]];

        return myDriver.executeInTransaction(() => {
            return MyPromise.construct<void>((resolve, reject) => {
                return Promise.resolve(cb())
                    .then(() => resolve())
                    .catch(reason => reject(reason));
            })
        });
    }

    get isConnected(): boolean {
        return this._isConnected;
    }

    onConnect(cb : () => Resolvable<any>) : void
    {
        this._connectListeners.push(cb);
        if (this.isConnected)
        {
            this._trigger(cb);
        }
    }

    table<TRow = Data>(metaTable: MetaTable): InternalTableDriver<TRow>
    {
        const driver = this._tableDrivers[metaTable.name];
        if (!driver) {
            throw new Error(`Driver not present for table ${metaTable.name}`);
        }

        if (!this._tableStatements[metaTable.name]) {
            this._tableStatements[metaTable.name] = new MySqlTableStatementStore(
                this.logger,
                metaTable,
                driver,
                this._isDebug,
            );
        }

        let queryCache : CacheStore<any, Partial<TRow>[]> | undefined = undefined;

        const tableData = this._tablesData.get(metaTable.name);
        if (tableData.cacheOptions) {
            if (!tableData.queryCache)
            {
                queryCache = new CacheStore<any, Partial<TRow>[]>(this.logger, tableData.cacheOptions!);
                tableData.queryCache = queryCache
            }
            else
            {
                queryCache = tableData.queryCache;
            }
        }

        return new MySqlTableImpl<TRow>(
            this.logger,
            metaTable,
            driver,
            metaTable.driverParams,
            this._isDebug,
            this._tableStatements[metaTable.name],
            queryCache
        );
    }

    private _newDriver(database: string) : MySqlDriver
    {
        const params = {
            database: database
        }
        const driver = new MySqlDriver(this.logger, params, this._isDebug);
        driver.onConnect(() => {
            this.logger.info('[_newDriver] Database %s is connected. isconnected=%s', database, driver.isConnected);
            this._determineConnected();
        })
        return driver;
    }

    private getTableDbName(table: MetaTable) : string
    {
        if (table.driverParams.database) {
            return table.driverParams.database;
        }
        if (process.env.MYSQL_DB) {
            return process.env.MYSQL_DB;
        }
        throw new Error(`DB not set for table ${table.name}`);
    }

    private _setupTableDB(table: MetaTable)
    {
        const db = this.getTableDbName(table);
        this.logger.info('[_setupTableDB] Table %s => DB %s', table.name, db);

        let driver = this._databases[db];
        if (!driver) {
            driver = this._newDriver(db);
            this._databases[db] = driver;
        }

        this._tableDrivers[table.name] = driver;
    }

    private _determineConnected()
    {
        const newIsConnected = _.every(_.values(this._databases), x => x.isConnected);
        this.logger.info('[_determineConnected] newIsConnected = %s', newIsConnected);
        if (this._isConnected == newIsConnected) {
            return;
        }
        this._isConnected = newIsConnected;
        if (this._isConnected) {
            this.logger.info('[_determineConnected] is connected');
            MyPromise.serial(this._connectListeners, x => this._trigger(x))
                .catch(reason => {
                    this.logger.error("[_determineConnected] ", reason);
                });
        }
    }

    private _exec<T>(cb : (driver: MySqlDriver) => T): Resolvable<T[]>
    {
        return MyPromise.serial(_.values(this._databases), x => {
            return cb(x);
        });
    }
    private _trigger(cb: ConnectFunc) : Resolvable<any>
    {
        return Promise.resolve(cb())
            .catch(reason => {
                this.logger.error("ERROR: ", reason);
            })
    }
}