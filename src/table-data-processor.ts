import _ from 'the-lodash';
import { Resolvable } from 'the-promise';
import { MetaTable } from './meta/meta-table';
import { MetaTableColumn } from './meta/meta-table-column';
import { FieldsOptions, Data } from './driver';

export class FieldsFilter {
    public useColumnFilter: boolean = false;
    public columns: MetaTableColumn[] = [];
}

export class TableDataProcessor {
    private _metaTable: MetaTable;

    constructor(metaTable: MetaTable) {
        this._metaTable = metaTable;
    }

    massageFieldsInfo(fieldsInfo?: FieldsOptions): FieldsFilter {
        const newFieldsInfo = new FieldsFilter();

        if (fieldsInfo) {
            if (fieldsInfo.fields) {
                const dict = _.makeDict(
                    fieldsInfo.fields,
                    (x) => x,
                    (x) => true,
                );
                for (const x of this._metaTable.columns) {
                    if (dict[x.name]) {
                        newFieldsInfo.columns.push(x);
                    }
                }
            }
        }
        newFieldsInfo.useColumnFilter = _.keys(newFieldsInfo.columns).length > 0;

        return newFieldsInfo;
    }

    buildScopeTarget(target?: Data): Data {
        let finalTarget: Data;
        if (target) {
            finalTarget = target;
        } else {
            finalTarget = {};
        }

        const adjustedData : Data = {}
        for (const key of _.keys(finalTarget)) {
            const column = this._metaTable.getColumn(key);
            const value = column.makeDbValue(finalTarget[column.name]);
            adjustedData[key] = value;
        }

        return adjustedData;
    }

    makeDbValue(field: string, value: Data): any {
        const columnMeta = this._metaTable.getColumn(field);
        if (columnMeta) {
            value = columnMeta.makeDbValue(value);
            return value;
        } else {
            return null;
        }
    }

    massageUserRow(data: any, fieldsInfo: FieldsFilter): Data | null {
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
            let value;
            if (column.isKey) {
                value = data._id[column.name];
            } else {
                value = data[column.name];
            }
            value = column.makeUserValue(value);
            resultData[column.name] = value;
        }
        return resultData;
    }
}
