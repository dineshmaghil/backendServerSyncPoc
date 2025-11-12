import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class SyncService {
  constructor(private readonly prisma: PrismaService) {}

  // 🔁 Pull data since last sync
  async pull(lastPulledAt: string) {
    const since = lastPulledAt ? new Date(Number(lastPulledAt)) : new Date(0);

    const newOrUpdated = await this.prisma.mh_off_orders.findMany({
      where: { updated_at: { gt: since } },
    });

    return {
      changes: {
        mh_off_orders: {
          created: newOrUpdated,
          updated: [],
          deleted: [],
        },
      },
      timestamp: Date.now(),
    };
  }

  // 📤 Push data from client
  async push(changes: any, lastPulledAt: string) {
    const orders = changes?.mh_off_orders ?? {};
    const { created = [], updated = [], deleted = [] } = orders;

    for (const item of created) {
      const sanitized = this.sanitizeOrderData(item);
      await this.prisma.mh_off_orders.upsert({
        where: { id: sanitized.id },
        create: sanitized,
        update: sanitized,
      });
    }

    for (const item of updated) {
      const sanitized = this.sanitizeOrderData(item);
      await this.prisma.mh_off_orders.update({
        where: { id: sanitized.id },
        data: sanitized,
      });
    }

    for (const id of deleted) {
      await this.prisma.mh_off_orders.delete({ where: { id } }).catch(() => {});
    }

    return { success: true };
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
}
