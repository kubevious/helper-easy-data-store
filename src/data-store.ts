import _ from 'the-lodash';
import { Promise, Resolvable } from 'the-promise';
import { ILogger } from 'the-logger';
import { MetaStore, MetaStoreBuilder, MetaStoreData } from './meta/meta-store';
import { IDriver, ITableDriver, Data, ConnectFunc, CacheOptions } from './driver';
import { MySQL } from './impl/mysql/driver';
import { MetaTable } from './meta/meta-table';
import { DataStoreTableAccessor } from './data-table-accessor';
import { TablesData } from './table-data';

class DriverInfo {
    logger: ILogger;
    name: string;
    driver: IDriver;

    constructor(logger: ILogger, name: string, driver: IDriver) {
        this.logger = logger;
        this.name = name;
        this.driver = driver;
    }
}

export interface ITableAccessor {
    table<TRow = Data>(nameOrAccessor: string | DataStoreTableAccessor<TRow>): ITableDriver<TRow>;
    executeInTransaction<T>(tables: (string | DataStoreTableAccessor<any>)[], cb: () => Resolvable<T>) : Promise<any>;
}

export class DataStore implements ITableAccessor {
    private logger: ILogger;
    private _metaStoreData = new MetaStoreData();
    private _meta: MetaStore = new MetaStore(this._metaStoreData);
    private _metaStoreBuilder: MetaStoreBuilder = new MetaStoreBuilder(this._meta, this._metaStoreData);
    private _isDebug: boolean;
    private _drivers: Record<string, DriverInfo> = {};
    private _mysqlDriver?: MySQL;
    private _isConnected = false;
    private _connectListeners : ConnectFunc[] = [];
    private _oneTimeConnectListeners : ConnectFunc[] = [];
    private _rootScope : ITableAccessor;
    private _tablesData : TablesData = new TablesData();

    constructor(logger: ILogger, isDebug: boolean) {
        this.logger = logger;
        this._isDebug = isDebug;
        this._rootScope = new DataStoreScope(this, this._meta, this._drivers, {});
    }

    get isDebug(): boolean {
        return this._isDebug;
    }

    get isConnected(): boolean {
        return this._isConnected;
    }

    get mysql() {
        return this._tryGetDriver('mysql');
    }

    onConnect(cb : () => Resolvable<any>) : void
    {
        this._connectListeners.push(cb);
        if (this.isConnected)
        {
            this._trigger(cb);
        }
    }

    meta(): MetaStoreBuilder {
        return this._metaStoreBuilder;
    }

    init() {
        this._setupDrivers();
        return this._driversExec((driver) => driver.connect());
    }

    private _setupDrivers()
    {
        const usedDrivers = this._extractUsedDriverAndTables();

        for (const name in usedDrivers) {
            this.logger.info('[init] Driver: %s...', name);

            const tables = usedDrivers[name];

            const logger = this.logger.sublogger('Driver' + _.upperFirst(name));
            let driver: IDriver;
            if (name == 'mysql') {
                this._mysqlDriver = new MySQL(logger, this._isDebug, tables, this._tablesData);
                driver = this._mysqlDriver;
            } else {
                throw new Error('Could not find driver for ' + name);
            }

            driver.onConnect(() => {
                this._determineConnected();
            })

            const driverInfo = new DriverInfo(logger, name, driver);

            this._drivers[name] = driverInfo;
        }
    }

    waitConnect() : Promise<void> {
        if (this.isConnected) {
            return Promise.resolve();
        }

        return Promise.construct<any>((resolve, reject) => {
            this._oneTimeConnectListeners.push(resolve);
        });
    }

    close() {

        for(const tableData of this._tablesData.getAll())
        {
            tableData.cacheOptions = undefined;
            if (tableData.queryCache) {
                tableData.queryCache.close();
                tableData.queryCache = undefined;
            }
        }

        return this._driversExec((driver) => driver.close())
    }

    setupCache<TRow = Data>(nameOrAccessor: string | DataStoreTableAccessor<TRow>, options? : Partial<CacheOptions>)
    {
        options = options || {};
        const myOptions : CacheOptions = {
            size: options.size || 1000,
            maxAgeMs: options.maxAgeMs || 1000 * 60 * 60
        };

        const tableData = this._tablesData.get(this._getTableName(nameOrAccessor));
        tableData.cacheOptions = myOptions;
    }

    table<TRow = Data>(nameOrAccessor: string | DataStoreTableAccessor<TRow>): ITableDriver<TRow>
    {
        return this._rootScope.table<TRow>(this._getTableName(nameOrAccessor))
    }

    private _getTableName<TRow = Data>(nameOrAccessor: string | DataStoreTableAccessor<TRow>) : string
    {
        if (_.isString(nameOrAccessor)) {
            return nameOrAccessor;
        } else {
            return nameOrAccessor.tableName;
        }
    }

    executeInTransaction<T>(tables: (string | DataStoreTableAccessor<any>)[], cb: () => Resolvable<T>) : Promise<any>
    {
        const tableMetas = tables.map(x => {
            if (_.isString(x)) {
                return this._meta.getTable(x)
            } else {
                return this._meta.getTable((<DataStoreTableAccessor<any>>x).tableName);
            }
        });
        const driverInfos : Record<string, DriverInfo> = {};
        for(const tableMeta of tableMetas) {
            if (!tableMeta.driverName) {
                throw new Error('Driver not specified.');
            }
            const driverInfo = this._drivers[tableMeta.driverName!];
            if (!driverInfo) {
                throw new Error(`Table not initialized: ${tableMeta.name}`);
            }
            driverInfos[tableMeta.driverName] = driverInfo;
        }

        if (_.keys(driverInfos).length == 0) {
            throw new Error('No Table Driver Found.');
        }
        if (_.keys(driverInfos).length > 1) {
            throw new Error('Cannot run transactions cross multiple drivers');
        }

        const driverInfo = this._drivers[_.keys(driverInfos)[0]];

        return driverInfo.driver.executeInTransaction(tableMetas, cb);
    }

    private _driversExec<T>(action: (driver: IDriver) => Resolvable<T>) {
        return Promise.parallel(_.values(this._drivers), (driverInfo) => {
            return action(driverInfo.driver);
        });
    }

    private _extractUsedDriverAndTables(): Record<string, MetaTable[]> {
        const x: Record<string, MetaTable[]> = {};
        for (const table of this._meta.tables) {
            if (!table.driverName) {
                throw new Error('Driver not set for table ' + table.name);
            }
            if (!x[table.driverName!]) {
                x[table.driverName!] = []
            }
            x[table.driverName!].push(table);
        }
        return x;
    }

    private _tryGetDriver(name: string): IDriver | null {
        const driverInfo = this._drivers[name];
        if (driverInfo) {
            return driverInfo.driver;
        }
        return null;
    }

    private _determineConnected()
    {
        this.logger.info("[_determineConnected] ");

        const newIsConnected = _.every(_.values(this._drivers), x => x.driver.isConnected);
        if (this._isConnected == newIsConnected) {
            return;
        }
        this._isConnected = newIsConnected;
        if (this._isConnected) {
            this.logger.info("[_determineConnected] is connected.");
            Promise.serial(this._connectListeners, x => this._trigger(x))
                .catch(reason => {
                    this.logger.error("[_determineConnected] ", reason);
                });

            for(const x of this._oneTimeConnectListeners)
            {
                this._trigger(x);
            }
            this._oneTimeConnectListeners = [];
        }
    }

    private _trigger(cb: ConnectFunc) : Resolvable<any>
    {
        return Promise.resolve(cb())
            .catch(reason => {
                this.logger.error("ERROR: ", reason);
            })
    }
}


class DataStoreScope implements ITableAccessor
{
    private _dataStore: DataStore;
    private _meta: MetaStore;
    private _drivers: Record<string, DriverInfo>;
    private _target: Data | undefined;

    constructor(dataStore: DataStore, meta: MetaStore, drivers: Record<string, DriverInfo>, target?: Data)
    {
        this._dataStore = dataStore;
        this._meta = meta;
        this._drivers = drivers;
        this._target = target;
    }

    executeInTransaction<T>(tables: (string | DataStoreTableAccessor<any>)[], cb: () => Resolvable<T>) : Promise<any>
    {
        return this._dataStore.executeInTransaction(tables, cb);
    }

    table<TRow = Data>(nameOrAccessor: string | DataStoreTableAccessor<TRow>): ITableDriver<TRow>
    {
        let tableName : string;
        if (_.isString(nameOrAccessor)) {
            tableName = nameOrAccessor;
        } else {
            tableName = nameOrAccessor.tableName;
        }
        const metaTable = this._meta.getTable(tableName);
        if (!metaTable.driverName) {
            throw new Error('Driver not specified.');
        }
        const driverInfo = this._drivers[metaTable.driverName!];
        if (!driverInfo) {
            throw new Error('Table not intialized: ' + tableName);
        }

        return driverInfo.driver.table(metaTable);
    }
}