import _ from 'the-lodash';
import { MetaTable, MetaTableBuilder } from './meta-table';

export type ConverterFunc = (value: any) => any;

export class MetaTableColumnData {
    public name: string = '';

    public toDbCb?: ConverterFunc;
    public fromDbCb?: ConverterFunc;

    public isSettable = true;
    public isAutoGeneratable = false;
    public isKey = false;

    public isPhysicalKey = false;
    public isComplexColumn = false;
}

export class MetaTableColumn {
    private _parent: MetaTable;
    private _data: MetaTableColumnData;

    constructor(parent: MetaTable, data: MetaTableColumnData) {
        this._parent = parent;
        this._data = data;
    }

    get name(): string {
        return this._data.name;
    }

    get isKey(): boolean {
        return this._data.isKey;
    }

    get isAutoGeneratable(): boolean {
        return this._data.isAutoGeneratable;
    }

    get isComplexColumn(): boolean {
        return this._data.isComplexColumn;
    }

    makeDbValue(value: any): any {
        if (this._data.toDbCb) {
            return this._data.toDbCb!(value);
        }
        return value;
    }

    makeUserValue(value: any): any {
        if (this._data.fromDbCb) {
            return this._data.fromDbCb!(value);
        }
        return value;
    }
}

export class MetaTableColumnBuilder {
    private parent: MetaTableBuilder;
    private data: MetaTableColumnData;

    constructor(parent: MetaTableBuilder, data: MetaTableColumnData) {
        this.parent = parent;
        this.data = data;
    }

    // Self Builder
    settable(): MetaTableColumnBuilder {
        this.data.isSettable = true;
        return this;
    }

    autogenerateable(): MetaTableColumnBuilder {
        this.data.isAutoGeneratable = true;
        return this;
    }

    complex(): MetaTableColumnBuilder {
        this.data.isComplexColumn = true;
        return this;
    }

    to(cb: ConverterFunc): MetaTableColumnBuilder {
        this.data.toDbCb = cb;
        return this;
    }

    from(cb: ConverterFunc): MetaTableColumnBuilder {
        this.data.fromDbCb = cb;
        return this;
    }

    // New Column Builder
    key(name: string): MetaTableColumnBuilder {
        return this.parent.key(name);
    }

    field(name: string): MetaTableColumnBuilder {
        return this.parent.field(name);
    }

    // Table Builder
    table(name: string): MetaTableBuilder {
        return this.parent.table(name);
    }
}
