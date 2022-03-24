import 'mocha';
import should = require('should');
import _ from 'the-lodash';
import { DataStore }  from '../../src';
import { setupLogger, LoggerOptions } from 'the-logger';

const loggerOptions = new LoggerOptions().enableFile(false).pretty(true);
const logger = setupLogger('test', loggerOptions);

function buildTestSuite(isDebug: boolean, driver: string, driverParams: any) {
    const testSuiteName = driver + '-data-store-basic' + (isDebug ? '-debug' : '');
    const testCasePrefix = testSuiteName + '::';

describe(testSuiteName, function() {

    it(testCasePrefix + 'constructor', function() {
        const dataStore = new DataStore(logger, isDebug);
        dataStore.close();
    });


    it(testCasePrefix + 'connect', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('customers')
                .driverParams(driverParams)
                .key('id')
                    .autogenerateable()
                .field('name')
                .field('email')
        ;
        
        dataStore.init();
        (dataStore.isConnected).should.be.false();
        return dataStore.waitConnect()
            .then(() => {
                (dataStore.isConnected).should.be.true();
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-empty', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('contacts')
                .driverParams(driverParams)
                .key('id')
                    .autogenerateable()
                .field('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('contacts');

        return dataStore.waitConnect()
            .then(() => dataStore.table('contacts').deleteMany())
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(0);
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-one-id-key', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('contacts')
                .driverParams(driverParams)
                .key('id')
                    .autogenerateable()
                .field('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('contacts');

        return dataStore.waitConnect()
            .then(() => dataStore.table('contacts').deleteMany())
            .then(() => dataStore.table('contacts').createNew({ name: 'john', email: 'john@doe.com'}))
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(1);
                (result[0].id).should.be.a.Number();
                (result[0].name).should.be.equal('john');
                (result[0].email).should.be.equal('john@doe.com');
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-multiple-id-key', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('contacts')
                .driverParams(driverParams)
                .key('id')
                    .autogenerateable()
                .field('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('contacts');

        return dataStore.waitConnect()
            .then(() => dataStore.table('contacts').deleteMany())
            .then(() => dataStore.table('contacts').createNew({ name: 'norris', email: 'chuck@norris.com'}))
            .then(() => dataStore.table('contacts').createNew({ name: 'john', email: 'john@doe.com'}))
            .then(() => dataStore.table('contacts').createNew({ name: 'john', email: 'john@wick.com'}))
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(3);

                {
                    const contact = _.find(result, x => x.name == 'norris');
                    should(contact).be.ok();
                    (contact!.id).should.be.a.Number();
                    (contact!.email).should.be.equal('chuck@norris.com');
                }
                {
                    const contact = _.find(result, x => x.email == 'john@doe.com');
                    should(contact).be.ok();
                    (contact!.id).should.be.a.Number();
                    (contact!.name).should.be.equal('john');
                }
                {
                    const contact = _.find(result, x => x.email == 'john@wick.com');
                    should(contact).be.ok();
                    (contact!.id).should.be.a.Number();
                    (contact!.name).should.be.equal('john');
                }
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-multiple-two-keys', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table<UserRow>('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(4);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.ok();
                    should(foo!.projectid).be.equal('coke');
                    should(foo!.email).be.equal('foo1@bar1.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo2');
                    should(foo).be.ok();
                    should(foo!.projectid).be.equal('pepsi');
                    should(foo!.email).be.equal('foo2@bar2.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo3');
                    should(foo).be.ok();
                    should(foo!.projectid).be.equal('coke');
                    should(foo!.email).be.equal('foo3@bar3.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo4');
                    should(foo).be.ok();
                    should(foo!.projectid).be.equal('pepsi');
                    should(foo!.email).be.equal('foo4@bar4.com');
                }
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-multiple-two-keys-selected-fields', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => table.queryMany({}, {
                fields: { fields: ['projectid', 'name'] }
            }))
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(4);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.deepEqual({
                        projectid: 'coke',
                        name: 'foo1'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo2');
                    should(foo).be.deepEqual({
                        projectid: 'pepsi',
                        name: 'foo2'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo3');
                    should(foo).be.deepEqual({
                        projectid: 'coke',
                        name: 'foo3'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo4');
                    should(foo).be.deepEqual({
                        projectid: 'pepsi',
                        name: 'foo4'
                    });
                }
            })
            .then(() => table.queryMany({}, {
                fields: { fields: ['name', 'projectid'] }
            }))
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(4);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.deepEqual({
                        projectid: 'coke',
                        name: 'foo1'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo2');
                    should(foo).be.deepEqual({
                        projectid: 'pepsi',
                        name: 'foo2'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo3');
                    should(foo).be.deepEqual({
                        projectid: 'coke',
                        name: 'foo3'
                    });
                }
                {
                    const foo = _.find(result, x => x.name == 'foo4');
                    should(foo).be.deepEqual({
                        projectid: 'pepsi',
                        name: 'foo4'
                    });
                }
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-multiple-filtered-two-keys', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => table.queryMany({projectid: 'pepsi'}))
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(2);

                {
                    const foo = _.find(result, x => x.name == 'foo2');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('pepsi');
                    (foo!.email).should.be.equal('foo2@bar2.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo4');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('pepsi');
                    (foo!.email).should.be.equal('foo4@bar4.com');
                }
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryMany-non-key-field', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo2', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo3', email: 'foo4@bar4.com'}))
            .then(() => table.queryMany({ email: 'foo1@bar1.com'}))
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(2);

                {
                    const coke = _.find(result, x => x.projectid == 'coke');
                    should(coke).be.ok();
                }
                {
                    const pepsi = _.find(result, x => x.projectid == 'pepsi');
                    should(pepsi).be.ok();
                }
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryOne-id-key', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('contacts')
                .driverParams(driverParams)
                .key('id')
                    .autogenerateable()
                .field('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('contacts');

        return dataStore.waitConnect()
            .then(() => dataStore.table('contacts').deleteMany())
            .then(() => dataStore.table('contacts').createNew({ name: 'norris', email: 'chuck@norris.com'}))
            .then(() => dataStore.table('contacts').createNew({ name: 'john', email: 'john@doe.com'}))
            .then(() => dataStore.table('contacts').createNew({ name: 'john', email: 'john@wick.com'}))
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(3);

                const wick = _.find(result, x => x.email == 'john@wick.com');
                return table.queryOne({ id : wick!.id });
            })
            .then(result => {
                should(result).be.ok();
                (result!.id).should.be.a.Number();
                (result!.name).should.be.equal('john');
                (result!.email).should.be.equal('john@wick.com');
            })
            .then(() => dataStore.close())
    });



    it(testCasePrefix + 'queryOne-id-key-selectfilter', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('contacts')
                .driverParams(driverParams)
                .key('id')
                    .autogenerateable()
                .field('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('contacts');

        return dataStore.waitConnect()
            .then(() => dataStore.table('contacts').deleteMany())
            .then(() => dataStore.table('contacts').createNew({ name: 'norris', email: 'chuck@norris.com'}))
            .then(() => dataStore.table('contacts').createNew({ name: 'john', email: 'john@doe.com'}))
            .then(() => dataStore.table('contacts').createNew({ name: 'john', email: 'john@wick.com'}))
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(3);

                const wick = _.find(result, x => x.email == 'john@wick.com');
                return table.queryOne({ id : wick!.id }, {
                    fields: { fields: ['email']}
                });
            })
            .then(result => {
                should(result).be.ok();
                should(result).be.eql({
                    email: 'john@wick.com'
                });
            })
            .then(() => dataStore.close())
    });
    

    it(testCasePrefix + 'queryOne-two-keys', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => table.queryOne({projectid: 'coke', name: 'foo3'}))
            .then(result => {
                should(result).be.ok();
                should(result).be.eql({
                    projectid: 'coke',
                    name: 'foo3',
                    email: 'foo3@bar3.com'
                });
            })
            .then(() => table.queryOne({name: 'foo3', projectid: 'coke'}))
            .then(result => {
                should(result).be.ok();
                should(result).be.eql({
                    projectid: 'coke',
                    name: 'foo3',
                    email: 'foo3@bar3.com'
                });
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryOne-two-keys-queryfilter', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => table.queryOne({projectid: 'coke', name: 'foo3'}, {
                fields: { fields: ['projectid', 'email']}
            }))
            .then(result => {
                should(result).be.ok();
                should(result).be.eql({
                    projectid: 'coke',
                    email: 'foo3@bar3.com'
                });
            })
            .then(() => table.queryOne({name: 'foo3', projectid: 'coke'}, {
                fields: { fields: ['email']}
            }))
            .then(result => {
                should(result).be.ok();
                should(result).be.eql({
                    email: 'foo3@bar3.com'
                });
            })
            .then(() => dataStore.close())
    });

    it(testCasePrefix + 'createNew-already-existing', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(result => {
                should(result).be.eql({
                    email: 'foo1@bar1.com',
                    projectid: 'coke',
                    name: 'foo1'
                });
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(1);
            })
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(result => {
                should(result).be.null();
            })
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'xxxxx'}))
            .then(result => {
                should(result).be.null();
            })
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(result => {
                should(result).be.eql({
                    email: 'foo2@bar2.com',
                    projectid: 'coke',
                    name: 'foo2'
                });
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(2);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.eql({
                        email: 'foo1@bar1.com',
                        projectid: 'coke',
                        name: 'foo1'
                    });
                }

                {
                    const foo = _.find(result, x => x.name == 'foo2');
                    should(foo).be.eql({
                        email: 'foo2@bar2.com',
                        projectid: 'coke',
                        name: 'foo2'
                    });
                }

            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'create', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').create({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(result => {
                should(result).be.eql({
                    email: 'foo1@bar1.com',
                    projectid: 'coke',
                    name: 'foo1'
                });
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(1);
                should(result[0]).be.eql({
                    email: 'foo1@bar1.com',
                    projectid: 'coke',
                    name: 'foo1'
                });
            })
            .then(() => dataStore.table('users').create({ projectid: 'coke', name: 'foo1', email: 'xxx'}))
            .then(result => {
                should(result).be.eql({
                    email: 'xxx',
                    projectid: 'coke',
                    name: 'foo1'
                });
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(1);
                should(result[0]).be.eql({
                    email: 'xxx',
                    projectid: 'coke',
                    name: 'foo1'
                });
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'update-two-keys', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').updateExisting({ projectid: 'coke', name: 'foo1', email: 'xxx'}))
            .then(result => {
                should(result).be.eql({
                    email: 'xxx',
                    projectid: 'coke',
                    name: 'foo1'
                });
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(2);
                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('xxx');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo2');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('foo2@bar2.com');
                }
            })
            .then(() => dataStore.table('users').updateExisting({ projectid: 'coke', name: 'foo11', email: 'yyy'}))
            .then(result => {
                should(result).be.null();
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(2);
                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('xxx');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo2');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('foo2@bar2.com');
                }
            })
            .then(() => dataStore.close())
    });



    it(testCasePrefix + 'delete', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => table.delete({ projectid: 'pepsi', name: 'foo2' }))
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(3);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('foo1@bar1.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo3');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('foo3@bar3.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo4');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('pepsi');
                    (foo!.email).should.be.equal('foo4@bar4.com');
                }
            })
            .then(() => dataStore.close())
    });



    it(testCasePrefix + 'delete-in-tx-success', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => {
                return dataStore.executeInTransaction(['users'], () => {
                    return table.delete({ projectid: 'pepsi', name: 'foo2' });
                })
            })
            .then(() => table.queryMany())
            .then(result => {
                should(result).be.an.Array();
                (result.length).should.be.equal(3);

                {
                    const foo = _.find(result, x => x.name == 'foo1');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('foo1@bar1.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo3');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('coke');
                    (foo!.email).should.be.equal('foo3@bar3.com');
                }
                {
                    const foo = _.find(result, x => x.name == 'foo4');
                    should(foo).be.ok();
                    (foo!.projectid).should.be.equal('pepsi');
                    (foo!.email).should.be.equal('foo4@bar4.com');
                }
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'delete-in-tx-failure', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('users')
                .driverParams(driverParams)
                .key('projectid')
                .key('name')
                .field('email')
        ;

        dataStore.init();

        let thereWasException = false;
        const table = dataStore.table('users');

        return dataStore.waitConnect()
            .then(() => dataStore.table('users').deleteMany())
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo1', email: 'foo1@bar1.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo2', email: 'foo2@bar2.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'coke', name: 'foo3', email: 'foo3@bar3.com'}))
            .then(() => dataStore.table('users').createNew({ projectid: 'pepsi', name: 'foo4', email: 'foo4@bar4.com'}))
            .then(() => {
                return dataStore.executeInTransaction(['users'], () => {
                    return table.delete({ projectid: 'pepsi', name: 'foo2' })
                        .then(() => {
                            throw new Error("I am causing tx rollback");
                        });
                })
                .catch(reason => {
                    thereWasException = true;
                })
            })
            .then(() => table.queryMany())
            .then(result => {
                should(thereWasException).be.true();

                should(result).be.an.Array();
                (result.length).should.be.equal(4);
            })
            .then(() => dataStore.close())
    });


    it(testCasePrefix + 'queryGroup', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('events')
                .driverParams(driverParams)
                .key('projectid')
                .key('message')
                .field('date')
        ;

        dataStore.init();

        const table = dataStore.table('events');

        return dataStore.waitConnect()
            .then(() => dataStore.table('events').deleteMany())
            .then(() => dataStore.table('events').createNew({ projectid: 'coke', message: 'foo1', date: '2020-08-10'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'pepsi', message: 'foo2', date: '2020-05-11'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'coke', message: 'foo3', date: '2020-05-12'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'pepsi', message: 'foo4', date: '2020-05-13'}))
            .then(() => table.queryGroup(['projectid']))
            .then(result => {
                should(result).be.an.Array();
                should(result.length).be.equal(2);
                should(_.some(result, x => x.projectid == 'coke')).be.true();
                should(_.some(result, x => x.projectid == 'pepsi')).be.true();
                for(const row of result)
                {
                    should(_.keys(row)).be.eql(['projectid']);
                }
            })
            .then(() => table.queryGroup(['projectid'], { projectid: 'pepsi' }))
            .then(result => {
                should(result).be.an.Array();
                should(result.length).be.equal(1);
                should(_.some(result, x => x.projectid == 'pepsi')).be.true();
                for(const row of result)
                {
                    should(_.keys(row)).be.eql(['projectid']);
                }
            })
            .then(() => table.queryGroup(['projectid'], {}, ['MAX(`date`) as maxDate']))
            .then(result => {
                should(result).be.an.Array();
                should(result.length).be.equal(2);
                {
                    const row = _.find(result, x => x.projectid == 'coke');
                    should(row).be.ok();
                    should(row!.projectid).be.equal('coke');
                    should(new Date(row!.maxDate).toISOString()).be.equal(new Date('2020-08-10').toISOString());
                }
                {
                    const row = _.find(result, x => x.projectid == 'pepsi');
                    should(row).be.ok();
                    should(row!.projectid).be.equal('pepsi');
                    should(new Date(row!.maxDate).toISOString()).be.equal(new Date('2020-05-13').toISOString());
                }
                for(const row of result)
                {
                    should(_.keys(row)).be.eql(['projectid', 'maxDate']);
                }
            })
            .then(() => table.queryGroup(['projectid'], { projectid: 'coke' }, ['MAX(`date`) as maxDate']))
            .then(result => {
                should(result).be.an.Array();
                should(result.length).be.equal(1);
                {
                    const row = _.find(result, x => x.projectid == 'coke');
                    should(row).be.ok();
                    should(row!.projectid).be.equal('coke');
                    should(new Date(row!.maxDate).toISOString()).be.equal(new Date('2020-08-10').toISOString());
                }
                for(const row of result)
                {
                    should(_.keys(row)).be.eql(['projectid', 'maxDate']);
                }
            })
            .then(() => dataStore.close())
            ;
    });




    it(testCasePrefix + '-field-filters', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('events')
                .driverParams(driverParams)
                .key('projectid')
                .key('message')
                .field('date')
        ;

        dataStore.init();

        const table = dataStore.table('events');

        return dataStore.waitConnect()
            .then(() => dataStore.table('events').deleteMany())
            .then(() => dataStore.table('events').createNew({ projectid: 'coke', message: 'foo1', date: '2020-08-10'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'pepsi', message: 'foo2', date: '2020-05-11'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'coke', message: 'bar3', date: '2020-05-12'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'pepsi', message: 'bar4', date: '2020-05-13'}))
            .then(() => table.queryMany({}, {
                filters: {
                    fields: [{
                        name: 'message',
                        operator: 'LIKE',
                        value: 'foo%'
                    }]
                }
            }))
            .then(result => {
                should(result).be.an.Array();
                should(result.length).be.equal(2);
                should(_.some(result, x => x.message == 'foo1')).be.true();
                should(_.some(result, x => x.message == 'foo2')).be.true();
            })
            .then(() => dataStore.close())
            ;
    });



    it(testCasePrefix + '-queryCount', function() {
        const dataStore = new DataStore(logger, isDebug);

        dataStore.meta()
            .table('events')
                .driverParams(driverParams)
                .key('projectid')
                .key('message')
                .field('date')
        ;

        dataStore.init();

        const table = dataStore.table('events');

        return dataStore.waitConnect()
            .then(() => dataStore.table('events').deleteMany())
            .then(() => dataStore.table('events').createNew({ projectid: 'coke', message: 'foo1', date: '2020-08-10'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'pepsi', message: 'foo2', date: '2020-05-11'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'coke', message: 'bar3', date: '2020-05-12'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'pepsi', message: 'bar4', date: '2020-05-13'}))
            .then(() => dataStore.table('events').createNew({ projectid: 'pepsi', message: 'bar5', date: '2020-05-13'}))
            .then(() => table.queryCount({}))
            .then(result => {
                should(result).be.equal(5);
            })
            .then(() => table.queryCount({ projectid: 'coke'}))
            .then(result => {
                should(result).be.equal(2);
            })
            .then(() => table.queryCount({ projectid: 'pepsi'}))
            .then(result => {
                should(result).be.equal(3);
            })
            .then(() => dataStore.close())
            ;
    });


});

}

buildTestSuite(false, 'mysql', { database: 'sample-db' });
// buildTestSuite(true, 'mysql', { database: 'sample-db' });


interface UserRow
{
    projectid: string,
    name: string,
    email: string
}