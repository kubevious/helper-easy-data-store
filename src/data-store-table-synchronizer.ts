import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger' ;

import { MetaTable } from './meta/meta-table' ;
import { DataStoreTable, DataItem } from './data-store-table' ;

import * as HashUtils from './utils/hash-utils';


export class DataStoreTableSynchronizer
{
    private _metaTable : MetaTable;
    private _dataStoreTable : DataStoreTable;
    private _logger : ILogger;

    private _filterValues?: Record<string, any>;
    private _skipDelete : boolean = false;

    constructor(parent : DataStoreTable, filterValues?: Record<string, any> | null , skipDelete? : boolean)
    {
        this._dataStoreTable = parent;
        this._metaTable = parent.metaTable;
        this._logger = parent.logger;
        if (filterValues) {
            this._filterValues = filterValues;
        }
        if (skipDelete) {
            this._skipDelete = true;
        }
    }

    execute(items : DataItem[]) : Promise<void>
    {
        return this._dataStoreTable.queryMany(this._filterValues)
            .then(currentItems => {

                let currentItemsDict : Record<string, { keys : ItemKeys, item: DataItem } > = {}
                for(let item of currentItems)
                {
                    let keys : ItemKeys = {};
                    for(var x of this._metaTable.getKeyFields())
                    {
                        keys[x] = item[x];
                        delete item[x];
                    }
                    currentItemsDict[HashUtils.calculateObjectHashStr(item)] = {
                        keys: keys,
                        item: item
                    }
                }

                var targetItemsDict : Record<string, DataItem> = {}
                for(var item of items)
                {
                    targetItemsDict[HashUtils.calculateObjectHashStr(item)] = item;
                }

                return this._productDelta(currentItemsDict, targetItemsDict);
            })
            .then(delta => {
                return this._executeDelta(delta);
            })
    }

    private _productDelta(currentItemsDict: Record<string, { keys : ItemKeys, item: DataItem } >,
        targetItemsDict: Record<string, DataItem>)
    {
        this._logger.verbose('currentItemsDict: ', currentItemsDict);
        this._logger.verbose('targetItemsDict: ', targetItemsDict);

        var delta : DeltaItem[] = [];

        if (!this._skipDelete) {
            for(var h of _.keys(currentItemsDict))
            {
                if (!targetItemsDict[h]) {
                    delta.push({
                        action: DeltaItemAction.Delete,
                        keys: currentItemsDict[h].keys
                    });
                }
            }
        }

        for(var h of _.keys(targetItemsDict))
        {
            if (!currentItemsDict[h]) {
                delta.push({
                    action: DeltaItemAction.Create,
                    item: targetItemsDict[h]
                });
            }
        }

        return delta;
    }

    private _executeDelta(delta : DeltaItem[]) : Promise<void>
    {
        this._logger.verbose('delta: ', delta);

        return Promise.serial(delta, this._executeDeltaItem.bind(this))
            .then(() => {});
    }

    private _executeDeltaItem(x : DeltaItem) : Promise<any>
    {
        // TODO: check the skipDelete Logic.
        if (x.action === DeltaItemAction.Create) {
            if (this._skipDelete) {
                return this._dataStoreTable.createOrUpdate(x.item!);
            } else {
                return this._dataStoreTable.create(x.item!);
            }
        } else if (x.action === DeltaItemAction.Delete) {
            return this._dataStoreTable.delete(x.keys!);
        } else {
            throw new Error('Not Implemented');
        }
    }
}

type ItemKeys = Record<string, any>;

interface DeltaItem 
{
    action: DeltaItemAction,
    keys?: ItemKeys,
    item?: DataItem
}

enum DeltaItemAction {
    Create = 1,
    Update = 2,
    Delete = 3,
  }