import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PrismaService.name);

  async onModuleInit() {
    await this.$connect();
    await this.ensureTableExists();
  }

  async onModuleDestroy() {
    await this.$disconnect();
  }

  private async ensureTableExists() {
    try {
      // Check if table exists by trying to query it
      await this.$queryRaw`SELECT 1 FROM mh_off_orders LIMIT 1`;
      this.logger.log('Table mh_off_orders exists');
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
        this.logger.warn('Table mh_off_orders does not exist. Creating it...');
        await this.$executeRawUnsafe(`
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
        this.logger.log('Table mh_off_orders created successfully');
      } else {
        this.logger.error('Unexpected error checking table existence', error);
        throw error;
      }
    }
  }
}
