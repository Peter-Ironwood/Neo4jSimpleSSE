import neo4j from 'neo4j-driver';

class Database {

    constructor() {
        this.driver = neo4j.driver( 'neo4j+s://demo.neo4jlabs.com', neo4j.auth.basic('goodreads', 'goodreads') );
    }

    async query(cypher, params = {}) {
        const session = this.driver.session();
        try {
            const result = await session.run(cypher, params);
            return result.records.map(record => record.toObject());
        } catch (error) {
            console.error('Query error:', error.message);
        } finally {
            await session.close();
        }
    }

    async close() {
        await this.driver.close();
    }
}

export default new Database();