import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PrismaService.name);

  // Helper to serialize BigInt values for JSON.stringify
  private serializeBigInt(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }
    if (typeof obj === 'bigint') {
      return obj.toString();
    }
    if (Array.isArray(obj)) {
      return obj.map(item => this.serializeBigInt(item));
    }
    if (typeof obj === 'object') {
      const serialized: any = {};
      for (const key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) {
          serialized[key] = this.serializeBigInt(obj[key]);
        }
      }
      return serialized;
    }
    return obj;
  }

  async onModuleInit() {
    try {
      // Test database connection
      await this.$connect();
      this.logger.log('✅ Database connection established');
      
      // Verify connection with a simple query
      const result = await this.$queryRawUnsafe('SELECT 1 as connected, DATABASE() as db_name, USER() as db_user') as any[];
      const serializedResult = this.serializeBigInt(result);
      this.logger.log('📊 Database connection verified:', JSON.stringify(serializedResult, null, 2));
      
      await this.ensureTableExists();
    } catch (error) {
      this.logger.error('❌ Failed to connect to database:', error);
      throw error;
    }
  }

  async onModuleDestroy() {
    await this.$disconnect();
    this.logger.log('🔌 Database connection closed');
  }

  private async ensureTableExists() {
    // Ensure mh_off_orders table exists
    await this.ensureTable('mh_off_orders', `
      CREATE TABLE IF NOT EXISTS \`mh_off_orders\` (
        \`id\` VARCHAR(36) NOT NULL,
        \`location_id\` VARCHAR(36) NOT NULL,
        \`customer_id\` VARCHAR(36) NULL,
        \`order_no\` VARCHAR(15) NOT NULL,
        \`order_type_id\` VARCHAR(36) NOT NULL,
        \`order_date\` DATE NOT NULL,
        \`order_time\` TIME(0) NOT NULL,
        \`ip_address\` VARCHAR(40) NOT NULL,
        \`user_agent\` VARCHAR(256) NOT NULL,
        \`updated_at\` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        PRIMARY KEY (\`id\`)
      ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    `);

    // Ensure mh_products table exists
    await this.ensureTable('mh_products', `
      CREATE TABLE IF NOT EXISTS \`mh_products\` (
        \`id\` VARCHAR(36) NOT NULL,
        \`product_code\` VARCHAR(50) NOT NULL,
        \`product_name\` VARCHAR(255) NOT NULL,
        \`description\` TEXT NULL,
        \`price\` DECIMAL(10, 2) NOT NULL,
        \`stock_quantity\` INT NOT NULL DEFAULT 0,
        \`is_active\` BOOLEAN NOT NULL DEFAULT true,
        \`updated_at\` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        PRIMARY KEY (\`id\`)
      ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    `);
  }

  private async ensureTable(tableName: string, createTableSQL: string) {
    try {
      // Check if table exists by trying to query it
      await this.$queryRawUnsafe(`SELECT 1 FROM \`${tableName}\` LIMIT 1`);
      this.logger.log(`Table ${tableName} exists`);
    } catch (error: any) {
      // If table doesn't exist, create it
      // P2010 = Raw query failed, 1146 = MySQL table doesn't exist error
      const isTableMissing = 
        error.code === 'P2010' || 
        error.code === 'P2021' || 
        error.meta?.code === '1146' ||
        error.message?.includes("doesn't exist") ||
        error.message?.includes('does not exist');
      
      if (isTableMissing) {
        this.logger.warn(`Table ${tableName} does not exist. Creating it...`);
        await this.$executeRawUnsafe(createTableSQL);
        this.logger.log(`Table ${tableName} created successfully`);
      } else {
        this.logger.error(`Unexpected error checking table existence for ${tableName}`, error);
        throw error;
      }
    }
  }
}
