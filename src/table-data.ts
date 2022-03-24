import _ from 'the-lodash';
import { CacheStore } from "@kubevious/helper-cache";
import { CacheOptions } from "./driver";

export class TablesData
{
    private _tables : Record<string, TableData> = {};

    get(name: string)
    {
        let data = this._tables[name];
        if (data) {
            return data;
        }
        data = new TableData();
        this._tables[name] = data;
        return data;
    }

    getAll() {
        return _.values(this._tables);
    }
}

export class TableData
{
    public cacheOptions? : CacheOptions;
    public queryCache? : CacheStore<any, any>;
}