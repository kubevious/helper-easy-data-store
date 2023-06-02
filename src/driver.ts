import { ILogger } from 'the-logger';
import { MetaTable } from './meta/meta-table';
import { Resolvable } from 'the-promise';
import { CacheStoreParams } from '@kubevious/helper-cache/dist/cache-store';

export type Data = Record<string, any>;

export type ConnectFunc = () => Resolvable<any>;

export interface IDriver {
    table<TRow = Data>(metaTable: MetaTable): InternalTableDriver<TRow>;

    connect(): Resolvable<any>;
    close(): Resolvable<any>;

    onConnect(cb : () => Resolvable<any>): void;

    executeInTransaction<T>(tableMetas: MetaTable[], cb: () => Resolvable<T>) : Promise<any>

    readonly isConnected: boolean;
}

export interface FieldsOptions {
    fields?: string[];
}

export interface FieldFilter {
    name: string;
    operator: string;
    value: string | number;
}

export interface FilterOptions {
    fields?: FieldFilter[];
}


export interface FieldOrder {
    name: string;
    asc: boolean;
}

export interface OrderOptions {
    fields?: FieldOrder[];
}

export interface QueryOptions {
    skipCache?: boolean;
    fields?: FieldsOptions;
    filters?: FilterOptions;
    order?: OrderOptions;
    limitCount?: number;
}

export interface QueryCountOptions {
    skipCache?: boolean;
}

export interface ITableDriver<TRow> {
    name: string;
    metaTable: MetaTable;
    driver: any;

    queryMany(target?: Partial<TRow>, options?: QueryOptions): Promise<Partial<TRow>[]>;

    queryOne(target: Partial<TRow>, options?: QueryOptions): Promise<Partial<TRow> | null>;

    queryGroup(groupFields: string[], target?: Partial<TRow>, aggregations?: string[]): Promise<Partial<TRow>[]>;

    queryCount(target?: Partial<TRow>, options?: QueryCountOptions) : Promise<number>

    create(data: Partial<TRow>): Promise<Partial<TRow>>;

    createNew(data: Partial<TRow>): Promise<Partial<TRow> | null>;

    updateExisting(data: Partial<TRow>): Promise<Partial<TRow> | null>;

    delete(data: Partial<TRow>): Promise<void>;

    deleteMany(data?: Partial<TRow>): Promise<void>;

    synchronizer(filterValues?: Partial<TRow>, skipDelete?: boolean): ISynchronizer<TRow>;
}


export interface InternalTableDriver<TRow> extends ITableDriver<TRow> {

}


export interface DeltaAction<TRow> {
    shouldCreate: boolean;
    item: Partial<TRow>;
}

export interface ISynchronizer<TRow> {
    execute(items: Partial<TRow>[]): Promise<DeltaAction<TRow>[]>;
}

export interface CacheOptions extends CacheStoreParams {
}