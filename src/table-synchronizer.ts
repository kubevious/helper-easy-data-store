import _ from 'the-lodash';
import { MyPromise } from 'the-promise';
import { calculateObjectHashStr } from './utils/hash-utils';
import { DeltaAction, ISynchronizer, ITableDriver } from './driver';
import { MetaTable } from './meta/meta-table';
import { ILogger } from 'the-logger';


interface CurrentItems<TRow>
{
    item: Partial<TRow>
    row: Partial<TRow>
}
export class TableSynchronizer<TRow> implements ISynchronizer<TRow> {
    private _dataStoreTable: ITableDriver<TRow>;
    private _metaTable: MetaTable;
    private logger: ILogger;
    private _filterValues: any;
    private _skipDelete: boolean;

    private _targetFields : string[];

    constructor(parent: ITableDriver<TRow>, metaTable: MetaTable, logger: ILogger, filterValues: any, skipDelete: boolean) {
        this._dataStoreTable = parent;
        this._metaTable = metaTable;
        this.logger = logger;
        this._filterValues = filterValues;
        this._skipDelete = skipDelete;

        this._targetFields = this._metaTable.columns
            .filter(x => !x.isAutoGeneratable)
            .map(x => x.name);
    }

    execute(items: Partial<TRow>[]): Promise<DeltaAction<TRow>[]> {
        return this._dataStoreTable
            .queryMany(this._filterValues)
            .then((currentRows) => {
                const currentItemsDict: Record<string, CurrentItems<TRow>> = {};
                for (const row of currentRows) {
                    const item = this._makeTargetObj(row);
                    currentItemsDict[calculateObjectHashStr(item)] = {
                        item: item,
                        row: row
                    };
                }

                const targetItemsDict: Record<string, Partial<TRow>> = {};
                for (const item of items) {
                    targetItemsDict[calculateObjectHashStr(item)] = item;
                }

                return this._produceDelta(currentItemsDict, targetItemsDict);
            })
            .then((delta) => {
                return this._executeDelta(delta)
                    .then(() => delta);
            });
    }

    private _makeTargetObj(row: Partial<TRow>): Partial<TRow>
    {
        const obj: Partial<TRow> = {};
        for (const x of this._targetFields) {
            (<any>obj)[x] = (<any>row)[x];
        }
        return obj;
    }

    private _produceDelta(
        currentItemsDict: Record<string, CurrentItems<TRow>>,
        targetItemsDict: Record<string, Partial<TRow>>): DeltaAction<TRow>[]
    {
        this.logger.verbose('[_produceDelta] currentItemsDict: ', currentItemsDict);
        this.logger.verbose('[_produceDelta] targetItemsDict: ', targetItemsDict);

        const delta: DeltaAction<TRow>[] = [];

        if (!this._skipDelete) {
            for (const h of _.keys(currentItemsDict)) {
                if (!targetItemsDict[h]) {
                    delta.push({
                        shouldCreate: false,
                        item: currentItemsDict[h].row
                    });
                }
            }
        }

        for (const h of _.keys(targetItemsDict)) {
            if (!currentItemsDict[h]) {
                delta.push({
                    shouldCreate: true,
                    item: targetItemsDict[h]
                });
            }
        }

        return delta;
    }

    private _executeDelta(delta: DeltaAction<TRow>[]): Promise<any> {
        this.logger.verbose('[_executeDelta] delta: ', delta);

        return MyPromise.serial<DeltaAction<TRow>, any>(delta, (x) => {
            if (x.shouldCreate) {
                return this._dataStoreTable.create(x.item);
            } else {
                return this._dataStoreTable.delete(x.item);
            }
        });
    }
}
