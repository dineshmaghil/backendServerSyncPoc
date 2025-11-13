import { Controller, Get, Post, Query, Body } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiQuery, ApiBody } from '@nestjs/swagger';
import { SyncService } from './sync.service';
import { PrismaService } from '../prisma/prisma.service';

@ApiTags('sync')
@Controller('sync')
export class SyncController {
  constructor(
    private readonly syncService: SyncService,
    private readonly prisma: PrismaService,
  ) {}

  @Get('health')
  @ApiOperation({ summary: 'Check database connection and health status' })
  @ApiResponse({ status: 200, description: 'Database connection is healthy' })
  @ApiResponse({ status: 500, description: 'Database connection failed' })
  async healthCheck() {
    try {
      // Helper to convert BigInt to number/string
      const convertBigInt = (value: any): any => {
        if (typeof value === 'bigint') {
          return Number(value);
        }
        if (Array.isArray(value)) {
          return value.map(convertBigInt);
        }
        if (value && typeof value === 'object') {
          const converted: any = {};
          for (const key in value) {
            converted[key] = convertBigInt(value[key]);
          }
          return converted;
        }
        return value;
      };

      // Test database connection
      const dbInfo = await this.prisma.$queryRawUnsafe(`
        SELECT 
          DATABASE() as database_name,
          USER() as user,
          CONNECTION_ID() as connection_id,
          NOW() as server_time
      `) as any[];
      
      // Check table existence
      const tables = await this.prisma.$queryRawUnsafe(`
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME IN ('mh_off_orders', 'mh_products')
      `) as any[];
      
      // Count records
      const orderCount = await this.prisma.$queryRawUnsafe(`SELECT COUNT(*) as count FROM mh_off_orders`) as any[];
      const productCount = await this.prisma.$queryRawUnsafe(`SELECT COUNT(*) as count FROM mh_products`) as any[];
      
      return {
        status: 'connected',
        database: convertBigInt(dbInfo[0]),
        tables: tables.map(t => t.TABLE_NAME),
        counts: {
          orders: convertBigInt(orderCount[0]?.count) || 0,
          products: convertBigInt(productCount[0]?.count) || 0,
        },
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      return {
        status: 'error',
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Get()
  @ApiOperation({ summary: 'Pull data from server since last sync' })
  @ApiQuery({ name: 'last_pulled_at', required: false, description: 'Timestamp of last sync (milliseconds since epoch)' })
  @ApiResponse({ status: 200, description: 'Returns changes since last sync' })
  async pull(@Query('last_pulled_at') lastPulledAt: string) {
    return this.syncService.pull(lastPulledAt);
  }

  @Post()
  @ApiOperation({ summary: 'Push data from client to server' })
  @ApiQuery({ name: 'last_pulled_at', required: false, description: 'Timestamp of last sync' })
  @ApiBody({
    description: 'Changes to sync',
    schema: {
      type: 'object',
      properties: {
        mh_off_orders: {
          type: 'object',
          properties: {
            created: { type: 'array', items: { type: 'object' } },
            updated: { type: 'array', items: { type: 'object' } },
            deleted: { type: 'array', items: { type: 'string' } },
          },
        },
        mh_products: {
          type: 'object',
          properties: {
            created: { type: 'array', items: { type: 'object' } },
            updated: { type: 'array', items: { type: 'object' } },
            deleted: { type: 'array', items: { type: 'string' } },
          },
        },
      },
    },
  })
  @ApiResponse({ status: 200, description: 'Data synced successfully' })
  @ApiResponse({ status: 400, description: 'Invalid request data' })
  async push(@Body() changes: any, @Query('last_pulled_at') lastPulledAt: string) {
    return this.syncService.push(changes, lastPulledAt);
  }
}

