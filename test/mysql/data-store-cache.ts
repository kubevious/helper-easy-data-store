import 'mocha';
import should = require('should');
import _ from 'the-lodash';
import { DataStore }  from '../../src';
import { setupLogger, LoggerOptions } from 'the-logger';

const loggerOptions = new LoggerOptions().enableFile(false).pretty(true);
const logger = setupLogger('test', loggerOptions);

function buildTestSuite(isDebug: boolean, driver: string, driverParams: any) {
    let testSuiteName = driver + '-data-store-cache' + (isDebug ? '-debug' : '');
    let testCasePrefix = testSuiteName + '::';

describe(testSuiteName, function() {

    it(testCasePrefix + 'queryMany-empty', function() {
        let dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.setupCache('users', {
            size: 100
        })


        dataStore.init();


        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(0);
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(0);
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-multiple', function() {
        let dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.setupCache('users', {
            size: 100
        })

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(2);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.eql({
                        projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo3');
                    should(foo).be.eql({
                        email: 'foo3@bar3.com',
                        projectid: 'coke', name: 'foo3'
                    });
                }
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(2);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.eql({
                        projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo3');
                    should(foo).be.eql({
                        email: 'foo3@bar3.com',
                        projectid: 'coke', name: 'foo3'
                    });
                }
            })
            .then(() => dataStore.close())
    });



    it(testCasePrefix + 'queryMany-multiple-customFields', function() {
        let dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.setupCache('users', {
            size: 100
        })

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo1', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => table.queryOne({ projectid: 'coke', name: 'foo1'}))
            .then(result => {
                should(result).be.eql({
                    projectid: 'coke',
                    name: 'foo1',
                    email: 'foo1@bar1.com'
                });
            })
            .then(() => table.queryOne({ projectid: 'pepsi', name: 'foo1'}))
            .then(result => {
                should(result).be.eql({
                    projectid: 'pepsi',
                    name: 'foo1',
                    email: 'foo2@bar2.com'
                });
            })
            .then(() => table.queryOne({ projectid: 'coke', name: 'foo1'}))
            .then(result => {
                should(result).be.eql({
                    projectid: 'coke',
                    name: 'foo1',
                    email: 'foo1@bar1.com'
                });
            })
            .then(() => table.queryOne({ projectid: 'pepsi', name: 'foo1'}))
            .then(result => {
                should(result).be.eql({
                    projectid: 'pepsi',
                    name: 'foo1',
                    email: 'foo2@bar2.com'
                });
            })
            .then(() => dataStore.close())
    });

});

}

buildTestSuite(false, 'mysql', { database: 'sample-db' });
// buildTestSuite(true, 'mysql', { database: 'sample-db' });
