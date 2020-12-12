import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { DataStore } from './data-store' ;
import { DataStoreTableSynchronizer } from './data-store-table-synchronizer' ;
import { MetaTable } from './meta/meta-table' ;
import { MySqlDriver } from '@kubevious/helper-mysql';


export type DataItem = Record<string, any>;

export class DataStoreTable
{
    private _parent : DataStore;
    private _mysqlDriver : MySqlDriver;
    private _metaTable : MetaTable;

    constructor(parent: DataStore, metaTable: MetaTable)
    {
        this._parent = parent;
        this._mysqlDriver = parent.mysql;
        this._metaTable = metaTable;
    }

    get name() {
        return this._metaTable.getName();
    }

    get logger() {
        return this._parent.logger;
    }

    get metaTable() {
        return this._metaTable;
    }

    synchronizer(filterValues?: Record<string, any> | null, skipDelete? : boolean) : DataStoreTableSynchronizer
    {
        return new DataStoreTableSynchronizer(this, filterValues, skipDelete);
    }

    queryMany(target? : Record<string, any> | null, fields?: string[]) : Promise<DataItem[]>
    {
        return this._queryItems(target, fields);
    }

    querySingle(target? : Record<string, any>, fields?: string[]) : Promise<DataItem | null>
    {
        return this._queryItems(target, fields)
            .then(result => {
                if (result.length == 0) {
                    return null;
                }
                return result[0];
            });
    }

    private _queryItems(target? : Record<string, any> | null, fields?: string[]): Promise<DataItem[]>
    {
        const myTarget = this._buildTarget(target);
        fields = fields || this._metaTable.getQueryFields();
        return this._execute<DataItem[]>(
            this._buildSelectSql(fields, _.keys(myTarget)),
            _.keys(myTarget),
            myTarget
        ).then(result => {
            return result.map(x => this._massageUserRow(x))
        })
    }

    create(data : DataItem)
    {
        const myData = this._buildTarget(data);
        return this._execute<any>(
            this._buildInsertSql(this._metaTable.getCreateFields()),
            this._metaTable.getCreateFields(),
            myData
        )
        .then(result => {
            if (result.insertId) {
                var keyColumn = this._metaTable.getKeyFields()[0];
                if (keyColumn)
                {
                    myData[keyColumn] = result.insertId;
                }
            }
            return myData;
        });
    }

    update(target : Record<string, any>, data : Record<string, any>) : Promise<void>
    {
        const myTarget = this._buildTarget(target);
        const fields = _.keys(data);
        const combinedData = _.defaults(_.clone(myTarget), data);
        return this._execute(
            this._buildUpdateSql(fields, _.keys(myTarget)),
            _.concat(
                fields,
                _.keys(myTarget)
            ),
            combinedData
        ).then(() => {});
    }

    createOrUpdate(data: DataItem) : Promise<DataItem>
    {
        const myData = this._buildTarget(data);
        return this._execute<any>(
            this._buildInsertOrUpdateSql(this._metaTable.getCreateFields(), this._metaTable.getUpdateFields()),
            _.concat(this._metaTable.getCreateFields(), this._metaTable.getUpdateFields()),
            myData
        )
        .then(result => {
            if (result.insertId) {
                var keyColumn = this._metaTable.getKeyFields()[0];
                if (keyColumn)
                {
                    myData[keyColumn] = result.insertId;
                }
            }
            return myData;
        });
    }

    delete(target?: Record<string, any>) : Promise<void>
    {
        const myTarget = this._buildTarget(target);
        return this._execute(
            this._buildDeleteSql(this._metaTable.getDeleteFields()),
            this._metaTable.getDeleteFields(),
            myTarget
        ).then(() => {});
    }

    private _buildSelectSql(what: string[], filters : string[]) : string
    {
        what = what || [];
        var fields = what.map(x => '`' + x + '`');

        filters = filters || [];
        var whereClause = '';
        if (filters.length > 0)
        {
            whereClause = ' WHERE ' + 
                filters.map(x => '`' + x + '` = ?').join(' AND ');
        }

        var sql = 'SELECT ' + 
            fields.join(', ') +
            ' FROM `' + this.name + '`' +
            whereClause + 
            ';'
        ;

        return sql;
    }

    private _buildInsertSql(what: string[]) : string
    {
        what = what || [];

        var fields = what.map(x => '`' + x + '`');

        var sql = 'INSERT INTO ' + 
            '`' + this.name + '` (' +
            fields.join(', ') +
            ') VALUES (' + 
            fields.map(x => '?').join(', ') +
            ');';

        return sql;
    }

    private _buildUpdateSql(what: string[], filters: string[]) : string
    {
        what = what || [];
        var updateClause = 
            what.map(x => '`' + x + '` = ?').join(', ');

        filters = filters || [];
        var whereClause = 
            filters.map(x => '`' + x + '` = ?').join(' AND ');

        var sql = 'UPDATE `' + this.name + '`' +
            ' SET ' +
            updateClause + 
            ' WHERE ' + 
            whereClause + 
            ';'
        ;

        return sql;
    }

    private _buildInsertOrUpdateSql(whatToCreate: string[], whatToUpdate: string[])
    {
        var createFields = whatToCreate.map(x => '`' + x + '`');

        var updateClause = 
            whatToUpdate.map(x => '`' + x + '` = ?').join(', ');

        var sql = 'INSERT INTO ' + 
            '`' + this.name + '` (' +
            createFields.join(', ') +
            ') VALUES (' + 
            createFields.map(x => '?').join(', ') +
            ') ' + 
            ' ON DUPLICATE KEY UPDATE ' + 
            updateClause +
            ';';

        return sql;
    }

    private _buildDeleteSql(filters: string[]) : string
    {
        filters = filters || [];

        var sql = 'DELETE FROM ' + 
            '`' + this.name + '` ' +
            ' WHERE ' +
            filters.map(x => '`' + x + '` = ?').join(' AND ');
            ');';

        return sql;
    }

    private _buildTarget(target?: Record<string, any> | null) : Record<string, any>
    {
        return target || {};
    }

    private _execute<T>(sql: string, fields : string[], data : Record<string, any>) : Promise<T>
    {
        var statement = this._mysqlDriver.statement(sql);
        var params = fields.map(x => {
            var columnMeta = this._metaTable.getColumn(x);
            var value = data[x];
            value = columnMeta._makeDbValue(value);
            return value;
        });
        return statement.execute(params);
    }

    private _massageUserRow(row: DataItem) : DataItem
    {
        for(var column of this._metaTable.getMassageableColumns())
        {
            var value = column._makeUserValue(row[column.name]);
            row[column.name] = value;
        }
        return row;
    }
}