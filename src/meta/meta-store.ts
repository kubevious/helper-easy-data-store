import _ from 'the-lodash';
import { MetaTable } from './meta-table';

export class MetaStore
{
    private _tables : Record<string, MetaTable> = {};

    constructor()
    {

    }

    getTable(name: string) : MetaTable
    {
        let table = this._tables[name];
        if (!table) {
            throw new Error("Unknown table: " + name);
        } 
        return table;
    }

    table(name: string) : MetaTable
    {
        var table = new MetaTable(this, name);
        this._tables[name] = table;
        return table;
    }
    
}