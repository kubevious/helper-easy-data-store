import _ from 'the-lodash';
import { MetaTable } from './meta-table';

export type ValueConverter = (value: any) => any;

export class MetaTableColumn
{
    private _parent : MetaTable;
    private _name : string;

    public isSettable = true;
    public isKey = false;

    private _toDbCb? : ValueConverter;
    private _fromDbCb? : ValueConverter;

    constructor(parent : MetaTable, name: string)
    {
        this._parent = parent;
        this._name = name;

    }

    get name() {
        return this._name;
    }

    get hasFromDbCb() : boolean {
        return _.isNotNullOrUndefined(this._fromDbCb);
    }

    table(name: string)
    {
        return this._parent.table(name);
    }
    
    key(name: string)
    {
        return this._parent.key(name);
    }

    field(name: string)
    {
        return this._parent.field(name);
    }

    settable()
    {
        this.isSettable = true;
        return this;
    }

    to(cb: ValueConverter)
    {
        this._toDbCb = cb;
        return this;
    }

    from(cb: ValueConverter)
    {
        this._fromDbCb = cb;
        return this;
    }

    _makeDbValue(value: any)
    {
        if (this._toDbCb) {
            return this._toDbCb!(value);
        }
        return value;
    }

    _makeUserValue(value: any)
    {
        if (this._fromDbCb) {
            return this._fromDbCb!(value);
        }
        return value;
    }
}