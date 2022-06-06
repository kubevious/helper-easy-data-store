import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger';

import { CacheStore } from '@kubevious/helper-cache';

import { MySqlDriver } from '@kubevious/helper-mysql';
import { MetaTable } from '../../meta/meta-table';
import { InternalTableDriver, ISynchronizer, Data, QueryOptions, QueryCountOptions } from '../../driver';
import { MetaTableColumn } from '../../meta/meta-table-column';

import { TableSynchronizer } from '../../table-synchronizer';
import { TableDataProcessor, FieldsFilter } from '../../table-data-processor';

import { MySqlTableStatementStore } from './statement-store';


interface ColumnValue {
    column: MetaTableColumn;
    value: any;
}

export class MySqlTableImpl<TRow = Data> implements InternalTableDriver<TRow>
{
    private logger: ILogger;
    private _name: string;
    private _isDebug: boolean;
    private _driver: MySqlDriver;
    private _metaTable: MetaTable;
    private _params: any;
    private _statementStore: MySqlTableStatementStore;

    private _autoIncrementColumn?: MetaTableColumn;
    private _insertColumns: MetaTableColumn[] = [];
    private _tableDataProcessor: TableDataProcessor;

    private _queryCache? : CacheStore<any, Partial<TRow>[]>;

    constructor(
        logger: ILogger,
        metaTable: MetaTable,
        driver: MySqlDriver,
        driverParams: any,
        isDebug: boolean,
        statementStore: MySqlTableStatementStore,
        queryCache?: CacheStore<any, Partial<TRow>[]>
    ) {
        this.logger = logger;
        this._driver = driver;
        this._metaTable = metaTable;
        this._isDebug = isDebug;
        this._statementStore = statementStore;
        this._queryCache = queryCache;

        this._name = this._metaTable.name;

        this._params = driverParams || {};

        this._tableDataProcessor = new TableDataProcessor(this._metaTable);

        this._insertColumns = this._statementStore.insertColumns;

        this._autoIncrementColumn = _.first(this._metaTable.keyColumns.filter((x) => x.isAutoGeneratable));
    }

    get name(): string {
        return this._name;
    }

    get isDebug(): boolean {
        return this._isDebug;
    }

    get metaTable(): MetaTable {
        return this._metaTable;
    }

    get driver(): MySqlDriver {
        return this._driver;
    }

    queryMany(target?: Partial<TRow>, options?: QueryOptions): Promise<Partial<TRow>[]>
    {
        if (this.isDebug) {
            this.logger.info('[queryMany] %s. Target: ', this.name, target);
        }

        return this._executeSelect(target, options);
    }

    queryOne(target: Partial<TRow>, options?: QueryOptions): Promise<Partial<TRow> | null>
    {
        if (this.isDebug) {
            this.logger.info('[queryOne] %s. Target: ', this.name, target);
        }

        return this._executeSelect(target, options).then((results) => {
            if (results.length == 0) {
                return null;
            }
            return results[0];
        });
    }

    queryGroup(groupFields: string[], target?: Partial<TRow>, aggregations?: string[]): Promise<Partial<TRow>[]>
    {
        if (this.isDebug) {
            this.logger.info('[queryGroup] %s. Target, GroupFields, Aggregations : ', this.name, target, groupFields, aggregations);
        }

        const groupColumns = groupFields.map(x => this._metaTable.getColumn(x));

        const queryFilters = this._makeQueryFilter(target);
        const queryColumns = queryFilters.map((x) => x.column);
        const queryValues = queryFilters.map((x) => x.value);

        const name = this._statementStore.getQueryGroupStatementName(queryColumns, groupColumns, aggregations );
        return this._executeDynamicStatement(name, queryValues, () => {
            return this._statementStore.buildGroupStatement(queryColumns, groupColumns, aggregations)
        }).then((results) => {
            return <Partial<TRow>[]> results;
        });
    }

    queryCount(target?: Partial<TRow>, options?: QueryCountOptions) : Promise<number>
    {
        if (this.isDebug) {
            this.logger.info('[queryCount] %s. Target: ', this.name, target);
        }

        const queryFilters = this._makeQueryFilter(target);
        const queryColumns = queryFilters.map((x) => x.column);
        const queryValues = queryFilters.map((x) => x.value);

        const name = this._statementStore.getQueryCountStatementName(queryColumns);
        return this._executeDynamicStatement(name, queryValues, () => {
            return this._statementStore.buildCountStatement(queryColumns);
        }).then((results) => {
            if (!results) {
                return 0;
            }
            if (!results[0]) {
                return 0;
            }
            return results[0]?.count ?? 0;
        });
    }

    create(data: Partial<TRow>): Promise<Partial<TRow>>
    {
        if (this.isDebug) {
            this.logger.info('[create] %s. Target: ', this.name, data);
        }

        return this.createNew(data).then((result) => {
            if (result == null) {
                return this.updateExisting(data)
                    .then(updateResult => {
                        return updateResult!;
                    });
            }
            return result!;
        });
    }

    createNew(data: Partial<TRow>): Promise<Partial<TRow> | null>
    {
        if (this.isDebug) {
            this.logger.info('[createNew] %s. Target: ', this.name, data);
        }

        const finalScope = this._tableDataProcessor.buildScopeTarget(data);

        const values = this._makeValuesArray(this._insertColumns, finalScope);

        return this._executeDynamicStatement<any>('DS_INSERT', values, () => {
            return this._statementStore.buildInsertStatement()
        }).then((result) => {
            if (result.affectedRows == 0) {
                return null;
            }
            const resultData = _.clone(finalScope);
            if (this._autoIncrementColumn) {
                resultData[this._autoIncrementColumn!.name] = result.insertId;
            }
            return <Partial<TRow>>resultData;
        });
    }

    updateExisting(data: Partial<TRow>): Promise<Partial<TRow> | null>
    {
        if (this.isDebug) {
            this.logger.info('[updateExisting] %s. Target: ', this.name, data);
        }

        const finalScope = this._tableDataProcessor.buildScopeTarget(data);

        const values: any[] = [
            ...this._makeValuesArray(this._statementStore.updateableColumns, finalScope),
            ...this._makeValuesArray(this._statementStore.updateFilterColumns, finalScope),
        ];

        return this._executeDynamicStatement<any>('DS_UPDATE', values, () => {
            return this._statementStore.buildUpdateStatement()
        }).then((result) => {
            if (result.affectedRows == 0) {
                return null;
            }
            return <Partial<TRow>>finalScope;
        });
    }

    delete(data: Partial<TRow>): Promise<void>
    {
        if (this.isDebug) {
            this.logger.info('[delete] %s. Target: ', this.name, data);
        }
        const queryFilters = this._makeGenericQueryFilter(this._metaTable.keyColumns, data);
        return this._executeDelete(queryFilters).then(() => {});
    }

    deleteMany(data?: Partial<TRow>): Promise<void>
    {
        if (this.isDebug) {
            this.logger.info('[deleteMany] %s. Target: ', this.name, data);
        }
        const queryFilters = this._makeQueryFilter(data);
        return this._executeDelete(queryFilters).then(() => {});
    }

    synchronizer(filterValues?: Partial<TRow>, skipDelete?: boolean): ISynchronizer<TRow>
    {
        return new TableSynchronizer(this, this.metaTable, this.logger, filterValues, skipDelete ?? false);
    }

    private _executeSelect(target?: Partial<TRow>, options?: QueryOptions): Promise<Partial<TRow>[]>
    {
        options = options || {}

        const finalFieldsInfo = this._tableDataProcessor.massageFieldsInfo(options.fields);
        const queryFilters = this._makeQueryFilter(target);

        let selectColumns: MetaTableColumn[];
        if (finalFieldsInfo.useColumnFilter) {
            selectColumns = finalFieldsInfo.columns;
        } else {
            selectColumns = this._metaTable.columns;
        }

        const queryColumns = queryFilters.map((x) => x.column);
        const queryValues = queryFilters.map((x) => x.value);

        const query = () => {
            return this._executeSelectStatement(
                queryColumns,
                selectColumns,
                queryValues,
                options!)
            .then((results) => {
                return results.map((x) => this._massageUserRow(x, finalFieldsInfo)!);
            });
        }

        if (this._queryCache && !options.skipCache) {
            const queryKey = {
                columns: queryColumns.map(x => x.name),
                values: queryValues,
                fields: options.fields,
                filters: options.filters,
            }

            return this._queryCache.dynamicGet(queryKey, (key) => query())
                .then(results => {
                    return results!;
                });
        }

        return query();
    }

    private _executeSelectStatement(
        queryColumns: MetaTableColumn[],
        selectColumns: MetaTableColumn[],
        queryValues: any[],
        options: QueryOptions) : Promise<any[]>
    {
        if (!this._isCacheableStatementSelect(options)) {
            const sql = this._statementStore.constructSelectSQL(
                queryColumns,
                selectColumns,
                options.filters,
                options.order,
                options.limitCount);
            return this._driver.executeSql(sql, queryValues);
        }

        const name = this._statementStore.getQueryStatementName(queryColumns, selectColumns);
        return this._executeDynamicStatement(name, queryValues, () => {
            return this._statementStore.constructSelectSQL(
                queryColumns,
                selectColumns)
        })
    }

    private _isCacheableStatementSelect(options: QueryOptions)
    {
        if (options.filters?.fields) {
            if (options.filters?.fields.length > 0) {
                return false;
            }
        }

        if (options.order?.fields) {
            if (options.order?.fields.length > 0) {
                return false;
            }
        }

        if (options.limitCount) {
            return false;
        }

        return true;
    }

    private _executeDelete(queryFilters: ColumnValue[]): Promise<any[]> {
        const queryColumns = queryFilters.map((x) => x.column);
        const queryValues = queryFilters.map((x) => x.value);

        const statement = this._statementStore.getDeleteStatement(queryColumns);
        return statement.execute(queryValues);
    }

    private _executeDynamicStatement<TResult = any[]>(name: string, params: any[], querySqlGetter : () => string): Promise<TResult> {
        if (this._isDebug) {
            this.logger.info('[_executeStatement] %s => %s', name, params);
        }

        let statement = this._statementStore.getByName(name);
        if (!statement) {
            const sql = querySqlGetter();
            statement = this._statementStore.registerStatement(name, sql);
            if (!statement) {
                throw new Error(`Dynamic statement ${name} not created.`);
            }
        }
        return statement.execute(params);
    }

    private _massageUserRow(data: any, fieldsInfo: FieldsFilter): Partial<TRow> | null {
        if (!data) {
            return null;
        }
        const resultData: Data = {};
        let columns: MetaTableColumn[];
        if (fieldsInfo.useColumnFilter) {
            columns = fieldsInfo.columns;
        } else {
            columns = this._metaTable.columns;
        }

        for (const column of columns) {
            let value = data[column.name];
            value = column.makeUserValue(value);
            resultData[column.name] = value;
        }
        return <Partial<TRow>>resultData;
    }

    private _makeGenericQueryFilter(columns: MetaTableColumn[], target?: Data): ColumnValue[] {
        const finalTarget = this._tableDataProcessor.buildScopeTarget(target);
        if (this.isDebug) {
            this.logger.info('[_makeQueryFilter] target: ', finalTarget);
        }

        const filterObj: ColumnValue[] = [];
        for (const columnMeta of columns) {
            let value = finalTarget[columnMeta.name];
            if (_.isNotNullOrUndefined(value)) {
                value = columnMeta.makeDbValue(value);
                filterObj.push({
                    column: columnMeta,
                    value: value,
                });
            }
        }
        return filterObj;
    }

    private _makeQueryFilter(target?: Data): ColumnValue[] {
        return this._makeGenericQueryFilter(this._metaTable.columns, target);
    }

    private _makeValuesArray(columns: MetaTableColumn[], data: Data): any[] {
        const values: any[] = [];

        for (const column of columns) {
            const value = column.makeDbValue(data[column.name]);
            values.push(value);
        }

        return values;
    }
}

