import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class SyncService {
  constructor(private readonly prisma: PrismaService) {}

  // 🔁 Pull data since last sync
  async pull(lastPulledAt: string) {
    const since = lastPulledAt ? new Date(Number(lastPulledAt)) : new Date(0);

    // Pull data from all tables
    const prisma = this.prisma as any;
    const [orders, products] = await Promise.all([
      prisma.mh_off_orders.findMany({
        where: { updated_at: { gt: since } },
      }),
      prisma.mh_products?.findMany({
        where: { updated_at: { gt: since } },
      }).catch(() => []), // Fallback if table doesn't exist yet
    ]);

    return {
      changes: {
        mh_off_orders: {
          created: orders || [],
          updated: [],
          deleted: [],
        },
        mh_products: {
          created: products || [],
          updated: [],
          deleted: [],
        },
      },
      timestamp: Date.now(),
    };
  }

  // 📤 Push data from client
  async push(changes: any, lastPulledAt: string) {
    // Handle mh_off_orders
    if (changes?.mh_off_orders) {
      await this.syncTable('mh_off_orders', changes.mh_off_orders, this.sanitizeOrderData.bind(this));
    }

    // Handle mh_products
    if (changes?.mh_products) {
      await this.syncTable('mh_products', changes.mh_products, this.sanitizeProductData.bind(this));
    }

    return { success: true };
  }

  // 🔄 Generic sync handler for any table
  private async syncTable(tableName: string, tableChanges: any, sanitizeFn: (item: any) => any) {
    const { created = [], updated = [], deleted = [] } = tableChanges;
    const prisma = this.prisma as any;
    const model = prisma[tableName];

    // Check if model exists in Prisma client
    if (!model) {
      console.warn(`Model ${tableName} not found in Prisma client. Using raw SQL fallback.`);
      // Fallback to raw SQL if model doesn't exist (e.g., Prisma client not regenerated)
      await this.syncTableRawSQL(tableName, tableChanges, sanitizeFn);
      return;
    }

    // Handle created items
    for (const item of created) {
      try {
        const sanitized = sanitizeFn(item);
        await model.upsert({
          where: { id: sanitized.id },
          create: sanitized,
          update: sanitized,
        });
      } catch (error) {
        console.error(`Error upserting ${tableName}:`, error);
        throw error;
      }
    }

    // Handle updated items
    for (const item of updated) {
      try {
        const sanitized = sanitizeFn(item);
        await model.update({
          where: { id: sanitized.id },
          data: sanitized,
        });
      } catch (error) {
        console.error(`Error updating ${tableName}:`, error);
        throw error;
      }
    }

    // Handle deleted items
    for (const id of deleted) {
      try {
        await model.delete({ where: { id } });
      } catch (error) {
        // Silently ignore delete errors (item might not exist)
        console.warn(`Error deleting ${tableName} with id ${id}:`, error);
      }
    }
  }

  // 🔄 Fallback: Sync using raw SQL when Prisma model doesn't exist
  private async syncTableRawSQL(tableName: string, tableChanges: any, sanitizeFn: (item: any) => any) {
    const { created = [], updated = [], deleted = [] } = tableChanges;

    // Handle created/updated items with INSERT ... ON DUPLICATE KEY UPDATE
    for (const item of [...created, ...updated]) {
      try {
        const sanitized = sanitizeFn(item);
        
        if (tableName === 'mh_products') {
          // Escape values for SQL
          const escapeValue = (val: any): string => {
            if (val === null || val === undefined) return 'NULL';
            if (typeof val === 'string') return `'${val.replace(/'/g, "''")}'`;
            if (typeof val === 'boolean') return val ? '1' : '0';
            if (val instanceof Date) return `'${val.toISOString().slice(0, 19).replace('T', ' ')}'`;
            return String(val);
          };

          const fields = Object.keys(sanitized).map(f => `\`${f}\``).join(', ');
          const values = Object.values(sanitized).map(escapeValue).join(', ');
          const updates = Object.keys(sanitized)
            .filter(key => key !== 'id')
            .map(key => `\`${key}\` = VALUES(\`${key}\`)`)
            .join(', ');
          
          const sql = `INSERT INTO \`${tableName}\` (${fields}) VALUES (${values}) ON DUPLICATE KEY UPDATE ${updates}`;
          
          await this.prisma.$executeRawUnsafe(sql);
        } else {
          throw new Error(`Raw SQL sync not implemented for table: ${tableName}`);
        }
      } catch (error) {
        console.error(`Error syncing ${tableName} with raw SQL:`, error);
        throw error;
      }
    }

    // Handle deleted items
    for (const id of deleted) {
      try {
        const escapedId = typeof id === 'string' ? `'${id.replace(/'/g, "''")}'` : String(id);
        await this.prisma.$executeRawUnsafe(`DELETE FROM \`${tableName}\` WHERE \`id\` = ${escapedId}`);
      } catch (error) {
        console.warn(`Error deleting ${tableName} with id ${id}:`, error);
      }
    }
  }

  // 🧹 Sanitize order data: remove sync metadata and convert date/time fields
  private sanitizeOrderData(item: any) {
    const { _status, _changed, ...rest } = item;
    
    // Convert order_date from timestamp to Date
    const order_date = item.order_date 
      ? new Date(typeof item.order_date === 'number' ? item.order_date : new Date(item.order_date).getTime())
      : new Date();

    // Convert order_time from string to DateTime
    // If it's a string like "18:34:16", combine with order_date
    let order_time: Date;
    if (typeof item.order_time === 'string') {
      const [hours, minutes, seconds] = item.order_time.split(':').map(Number);
      order_time = new Date(order_date);
      order_time.setHours(hours || 0, minutes || 0, seconds || 0, 0);
    } else if (typeof item.order_time === 'number') {
      order_time = new Date(item.order_time);
    } else {
      order_time = new Date();
    }

    // Convert updated_at from timestamp to Date (or use current time)
    const updated_at = item.updated_at 
      ? new Date(typeof item.updated_at === 'number' ? item.updated_at : new Date(item.updated_at).getTime())
      : new Date();

    return {
      ...rest,
      order_date,
      order_time,
      updated_at,
    };
  }

  // 🧹 Sanitize product data: remove sync metadata and convert fields
  private sanitizeProductData(item: any) {
    const { _status, _changed, ...rest } = item;

    // Convert price to Decimal (Prisma Decimal type)
    const price = typeof item.price === 'number' ? item.price : parseFloat(item.price) || 0;

    // Convert stock_quantity to integer
    const stock_quantity = typeof item.stock_quantity === 'number' 
      ? Math.floor(item.stock_quantity) 
      : parseInt(item.stock_quantity, 10) || 0;

    // Convert is_active to boolean
    const is_active = typeof item.is_active === 'boolean' 
      ? item.is_active 
      : item.is_active === 'true' || item.is_active === true || item.is_active === 1;

    // Convert updated_at from timestamp to Date (or use current time)
    const updated_at = item.updated_at 
      ? new Date(typeof item.updated_at === 'number' ? item.updated_at : new Date(item.updated_at).getTime())
      : new Date();

    return {
      ...rest,
      price,
      stock_quantity,
      is_active,
      updated_at,
    };
  }
}
