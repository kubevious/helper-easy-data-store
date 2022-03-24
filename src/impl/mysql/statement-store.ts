import _ from 'the-lodash';
import { ILogger } from 'the-logger';

import { MySqlDriver, MySqlStatement } from '@kubevious/helper-mysql';
import { MetaTable } from '../../meta/meta-table';
import { FilterOptions } from '../../driver';
import { MetaTableColumn } from '../../meta/meta-table-column';

export class MySqlTableStatementStore {
    private logger: ILogger;
    private _name: string;
    private _isDebug: boolean;
    private driver: MySqlDriver;
    private metaTable: MetaTable;

    private _insertColumns: MetaTableColumn[] = [];
    private _updateableColumns: MetaTableColumn[] = [];
    private _updateFilterColumns: MetaTableColumn[] = [];

    private _statements: Record<string, MySqlStatement> = {};

    constructor(logger: ILogger, metaTable: MetaTable, driver: MySqlDriver, isDebug: boolean) {
        this.logger = logger;
        this.driver = driver;
        this.metaTable = metaTable;
        this._name = metaTable.name;
        this._isDebug = isDebug;

        this._insertColumns = this.metaTable.columns.filter((x) => !x.isAutoGeneratable);
        this._updateableColumns = this.metaTable.nonKeyColumns;
        this._updateFilterColumns = this.metaTable.keyColumns;
    }

    get insertColumns(): MetaTableColumn[] {
        return this._insertColumns;
    }

    get updateableColumns(): MetaTableColumn[] {
        return this._updateableColumns;
    }

    get updateFilterColumns(): MetaTableColumn[] {
        return this._updateFilterColumns;
    }

    public getByName(name: string): MySqlStatement {
        return this._statements[name];
    }

    buildInsertStatement() {
        let sql = `INSERT IGNORE INTO \`${this._name}\` (`;
        sql += this._insertColumns.map((x) => `\`${x.name}\``).join(', ');
        sql += ') VALUE (';
        sql += this._insertColumns.map((x) => `?`).join(', ');
        sql += ');';
        return sql;
    }

    buildUpdateStatement() {
        if (this._updateableColumns.length === 0) {
            throw new Error(`Table ${this._name} is not updateable.`);
        }
        let sql = `UPDATE \`${this._name}\` SET `;
        sql += this._updateableColumns.map((x) => `\`${x.name}\` = ?`).join(', ');
        sql += this._makeColumnsFilterSql(this._updateFilterColumns);
        sql += ';';
        return sql;
    }
    
    getDeleteStatement(columns: MetaTableColumn[])
    {
        const name = this.getDeleteStatementName(columns);
        const statement = this.getByName(name);
        if (statement) {
            return statement;
        }

        return this._prepareDeleteStatement(name, columns);
    }

    private _prepareDeleteStatement(name: string, columns: MetaTableColumn[]) {
        let sql = `DELETE FROM \`${this._name}\``;
        sql += this._makeColumnsFilterSql(columns);
        sql += ';';

        return this.registerStatement(name, sql);
    }

    public constructSelectSQL(
        filterColumnChoice?: MetaTableColumn[],
        selectColumnChoice?: MetaTableColumn[],
        fieldFilters?: FilterOptions)
    {
        let finalFilterColumns: MetaTableColumn[];
        if (filterColumnChoice) {
            finalFilterColumns = filterColumnChoice!;
        } else {
            finalFilterColumns = [];
        }

        let finalSelectColumns: MetaTableColumn[];
        if (selectColumnChoice) {
            finalSelectColumns = selectColumnChoice!;
        } else {
            finalSelectColumns = this.metaTable.columns;
        }

        let sql = 'SELECT ';
        sql += finalSelectColumns.map((x) => `\`${x.name}\``).join(', ');
        sql += ' FROM ';
        sql += `\`${this._name}\``;


        let filterCriteria : string[] = [];

        if (finalFilterColumns.length > 0)
        {
            filterCriteria = finalFilterColumns.map((x) => this._makeColumnFilterSql(x));
        }

        if (fieldFilters && fieldFilters.fields && fieldFilters.fields.length > 0)
        {
            const filters = fieldFilters.fields.map((x) => {
                if (_.isString(x.value)) {
                    return `(\`${x.name}\` ${x.operator} '${x.value}')`
                } else {
                    return `(\`${x.name}\` ${x.operator} ${x.value})`
                }
            });
            filterCriteria = _.concat(filterCriteria, filters);
        }

        if (filterCriteria.length > 0) {
            sql += ' WHERE ';
            sql += filterCriteria.join(' AND ');
        }

        sql += ';';
        return sql;
    }

    public registerStatement(name: string, sql: string) {
        this.logger.info('[_registerStatement] %s => %s', name, sql);
        const statement = this.driver.statement(sql);
        this._statements[name] = statement;
        return statement;
    }

    public getDeleteStatementName(columns?: MetaTableColumn[]): string {
        return this._getStatementName('DELETE', columns);
    }

    public getQueryStatementName(
        finalFilterColumns?: MetaTableColumn[],
        finalSelectColumns?: MetaTableColumn[]
    ): string {
        let name = this._getStatementName('SELECT', finalFilterColumns);
        if (!finalSelectColumns || finalSelectColumns!.length == 0) {
            name += '_ALL';
        } else {
            name += '_SELECT_' + finalSelectColumns!.map((x) => x.name).join('_');
        }
        return name;
    }

    public getQueryGroupStatementName(
        finalFilterColumns?: MetaTableColumn[],
        finalGroupColumns?: MetaTableColumn[],
        aggregations?: string[]
    ): string {
        let name = this._getStatementName('GROUP', finalFilterColumns);
        if (!finalGroupColumns || finalGroupColumns!.length == 0) {
            name += '_GRALL';
        } else {
            name += '_GROUP_' + finalGroupColumns!.map((x) => x.name).join('_');
        }
        if (aggregations && aggregations!.length > 0) {
            name += '_AGGR_' + aggregations.join('_');
        }
        return name;
    }

    public getQueryCountStatementName(
        filterColumns?: MetaTableColumn[]
    ): string {
        const name = this._getStatementName('COUNT', filterColumns);
        return name;
    }

    public buildGroupStatement(
        finalFilterColumns: MetaTableColumn[],
        finalGroupColumns: MetaTableColumn[],
        aggregations?: string[]
    ) : string
    {
        const selectFields = 
            [
                ...finalGroupColumns.map(x => `\`${x.name}\``),
                ...aggregations || []
            ]

        let sql = 'SELECT ';
        sql += selectFields.join(', ');
        sql += ' FROM ';
        sql += `\`${this._name}\``;

        sql += this._makeColumnsFilterSql(finalFilterColumns);

        sql += ' GROUP BY '
        sql += finalGroupColumns.map(x => `\`${x.name}\``).join(', ');

        sql += ';';

        this.logger.debug("SQL:::: " , sql);
        return sql;
    }


    public buildCountStatement(
        finalFilterColumns: MetaTableColumn[],
    ) : string
    {
        let sql = 'SELECT COUNT(*) as count';
        sql += ' FROM ';
        sql += `\`${this._name}\``;

        sql += this._makeColumnsFilterSql(finalFilterColumns);
        sql += ';';

        this.logger.debug("SQL:::: " , sql);
        return sql;
    }

    private _getStatementName(kind: string, columns?: MetaTableColumn[]): string {
        if (columns && columns!.length > 0) {
            return `DS_${kind}_WHERE_${columns!.map((x) => x.name).join('_')}`;
        } else {
            return `DS_${kind}_ALL`;
        }
    }

    private _makeColumnsFilterSql(columnsMeta: MetaTableColumn[]) : string
    {
        if (!columnsMeta || columnsMeta.length == 0) {
            return '';
        }

        const sql = ' WHERE ' + columnsMeta.map((x) => this._makeColumnFilterSql(x)).join(' AND ');
        return sql;
    }

    private _makeColumnFilterSql(columnMeta: MetaTableColumn) : string
    {
        if (columnMeta.isComplexColumn) {
            return `(\`${columnMeta.name}\` = CAST(? as JSON))`
        }
        return `(\`${columnMeta.name}\` = ?)`
    }
}
