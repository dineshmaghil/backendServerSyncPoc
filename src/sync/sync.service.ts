import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class SyncService {
  constructor(private readonly prisma: PrismaService) {}

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
    console.log(`[Sync] Received push request with changes:`, JSON.stringify(changes, null, 2));
    
    // Handle mh_off_orders
    if (changes?.mh_off_orders) {
      console.log(`[Sync] Processing mh_off_orders`);
      await this.syncTable('mh_off_orders', changes.mh_off_orders, this.sanitizeOrderData.bind(this));
    }

    // Handle mh_products
    if (changes?.mh_products) {
      console.log(`[Sync] Processing mh_products`);
      await this.syncTable('mh_products', changes.mh_products, this.sanitizeProductData.bind(this));
    }

    console.log(`[Sync] Push completed successfully`);
    return { success: true };
  }

  // 🔄 Generic sync handler for any table
  private async syncTable(tableName: string, tableChanges: any, sanitizeFn: (item: any) => any) {
    const { created = [], updated = [], deleted = [] } = tableChanges;
    console.log(`[Sync] syncTable called for ${tableName} - created: ${created.length}, updated: ${updated.length}, deleted: ${deleted.length}`);
    
    const prisma = this.prisma as any;
    const model = prisma[tableName];

    // Check if model exists in Prisma client
    if (!model) {
      console.warn(`[Sync] Model ${tableName} not found in Prisma client. Using raw SQL fallback.`);
      // Fallback to raw SQL if model doesn't exist (e.g., Prisma client not regenerated)
      await this.syncTableRawSQL(tableName, tableChanges, sanitizeFn);
      return;
    }

    console.log(`[Sync] Using Prisma model for ${tableName}`);

    // Handle created items
    for (const item of created) {
      try {
        const sanitized = sanitizeFn(item);
        console.log(`[Sync] Upserting ${tableName} item:`, sanitized.id);
        console.log(`[Sync] 📦 Data being inserted:`, JSON.stringify(sanitized, null, 2));
        const result = await model.upsert({
          where: { id: sanitized.id },
          create: sanitized,
          update: sanitized,
        });
        console.log(`[Sync] ✅ Successfully upserted ${tableName} item:`, sanitized.id);
        const serializedResult = this.serializeBigInt(result);
        console.log(`[Sync] 🔍 Inserted record:`, JSON.stringify(serializedResult, null, 2));
      } catch (error) {
        console.error(`[Sync] ❌ Error upserting ${tableName}:`, error);
        console.error(`[Sync] Item that failed:`, JSON.stringify(item, null, 2));
        throw error;
      }
    }

    // Handle updated items
    for (const item of updated) {
      try {
        const sanitized = sanitizeFn(item);
        console.log(`[Sync] Updating ${tableName} item:`, sanitized.id);
        await model.update({
          where: { id: sanitized.id },
          data: sanitized,
        });
        console.log(`[Sync] Successfully updated ${tableName} item:`, sanitized.id);
      } catch (error) {
        console.error(`[Sync] Error updating ${tableName}:`, error);
        console.error(`[Sync] Item that failed:`, item);
        throw error;
      }
    }

    // Handle deleted items
    for (const id of deleted) {
      try {
        console.log(`[Sync] Deleting ${tableName} item:`, id);
        await model.delete({ where: { id } });
        console.log(`[Sync] Successfully deleted ${tableName} item:`, id);
      } catch (error) {
        // Silently ignore delete errors (item might not exist)
        console.warn(`[Sync] Error deleting ${tableName} with id ${id}:`, error);
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
        console.log(`[Sync] Processing ${tableName} item:`, sanitized);
        
        if (tableName === 'mh_products') {
          // Escape values for SQL
          const escapeValue = (val: any, fieldName: string): string => {
            if (val === null || val === undefined) {
              // Allow NULL for optional fields like description
              if (fieldName === 'description') return 'NULL';
              return 'NULL';
            }
            if (typeof val === 'string') {
              // Escape single quotes in strings
              return `'${val.replace(/'/g, "''").replace(/\\/g, '\\\\')}'`;
            }
            if (typeof val === 'boolean') return val ? '1' : '0';
            if (val instanceof Date) {
              // Format date for MySQL DATETIME: YYYY-MM-DD HH:MM:SS
              const year = val.getFullYear();
              const month = String(val.getMonth() + 1).padStart(2, '0');
              const day = String(val.getDate()).padStart(2, '0');
              const hours = String(val.getHours()).padStart(2, '0');
              const minutes = String(val.getMinutes()).padStart(2, '0');
              const seconds = String(val.getSeconds()).padStart(2, '0');
              return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
            }
            if (typeof val === 'number') {
              // Handle decimal numbers properly
              return String(val);
            }
            return String(val);
          };

          // Build field list and values
          const fields = Object.keys(sanitized).map(f => `\`${f}\``);
          const values = Object.entries(sanitized).map(([key, val]) => escapeValue(val, key));
          const updates = Object.keys(sanitized)
            .filter(key => key !== 'id')
            .map(key => `\`${key}\` = VALUES(\`${key}\`)`)
            .join(', ');
          
          const sql = `INSERT INTO \`${tableName}\` (${fields.join(', ')}) VALUES (${values.join(', ')}) ON DUPLICATE KEY UPDATE ${updates}`;
          
          console.log(`[Sync] 📝 Executing SQL for ${tableName}:`);
          console.log(`[Sync] SQL:`, sql);
          console.log(`[Sync] 📦 Data being inserted:`, JSON.stringify(sanitized, null, 2));
          
          const result = await this.prisma.$executeRawUnsafe(sql);
          console.log(`[Sync] ✅ SQL executed successfully. Rows affected:`, result);
          
          // Verify the data was inserted by querying it back
          try {
            const inserted = await this.prisma.$queryRawUnsafe(
              `SELECT * FROM \`${tableName}\` WHERE \`id\` = '${sanitized.id.replace(/'/g, "''")}'`
            ) as any[];
            const serializedInserted = this.serializeBigInt(inserted);
            console.log(`[Sync] 🔍 Verified inserted data:`, JSON.stringify(serializedInserted, null, 2));
          } catch (verifyError) {
            console.warn(`[Sync] ⚠️ Could not verify inserted data:`, verifyError);
          }
        } else {
          throw new Error(`Raw SQL sync not implemented for table: ${tableName}`);
        }
      } catch (error) {
        console.error(`[Sync] Error syncing ${tableName} with raw SQL:`, error);
        console.error(`[Sync] Item that failed:`, item);
        throw error;
      }
    }

    // Handle deleted items
    for (const id of deleted) {
      try {
        const escapedId = typeof id === 'string' ? `'${id.replace(/'/g, "''")}'` : String(id);
        await this.prisma.$executeRawUnsafe(`DELETE FROM \`${tableName}\` WHERE \`id\` = ${escapedId}`);
      } catch (error) {
        console.warn(`[Sync] Error deleting ${tableName} with id ${id}:`, error);
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
    const price = typeof item.price === 'number' ? item.price : parseFloat(String(item.price)) || 0;

    // Convert stock_quantity to integer
    const stock_quantity = typeof item.stock_quantity === 'number' 
      ? Math.floor(item.stock_quantity) 
      : parseInt(String(item.stock_quantity), 10) || 0;

    // Convert is_active to boolean
    const is_active = typeof item.is_active === 'boolean' 
      ? item.is_active 
      : item.is_active === 'true' || item.is_active === true || item.is_active === 1;

    // Convert updated_at from timestamp to Date (or use current time)
    let updated_at: Date;
    if (item.updated_at) {
      if (typeof item.updated_at === 'number') {
        updated_at = new Date(item.updated_at);
      } else if (item.updated_at instanceof Date) {
        updated_at = item.updated_at;
      } else {
        updated_at = new Date(String(item.updated_at));
      }
    } else {
      updated_at = new Date();
    }

    // Handle description - can be null/undefined/empty string
    const description = item.description && item.description.trim() !== '' ? item.description : null;

    return {
      id: item.id,
      product_code: item.product_code,
      product_name: item.product_name,
      description: description,
      price: price,
      stock_quantity: stock_quantity,
      is_active: is_active,
      updated_at: updated_at,
    };
  }
}
