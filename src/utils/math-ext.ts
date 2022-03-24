import _ from 'the-lodash';

export function choices<T>(input: T[], skipEmpty?: boolean): T[][] {
    const result: T[][] = [];
    if (!input.length) {
        return result;
    }

    const selector: number[] = Array(input.length).fill(0);
    if (skipEmpty) {
        selector[0] = 1;
    }

    while (_.last(selector)! <= 1) {
        const item: T[] = [];
        for (let i = 0; i < selector.length; i++) {
            if (selector[i] > 0) {
                item.push(input[i]);
            }
        }
        result.push(item);

        selector[0]++;
        for (let i = 0; i < selector.length - 1; i++) {
            if (selector[i] > 1) {
                selector[i] = 0;
                selector[i + 1]++;
            }
        }
    }

    return result;
}
