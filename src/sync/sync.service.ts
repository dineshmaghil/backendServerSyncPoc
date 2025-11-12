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
      await this.prisma.mh_off_orders.upsert({
        where: { id: item.id },
        create: item,
        update: item,
      });
    }

    for (const item of updated) {
      await this.prisma.mh_off_orders.update({
        where: { id: item.id },
        data: item,
      });
    }

    for (const id of deleted) {
      await this.prisma.mh_off_orders.delete({ where: { id } }).catch(() => {});
    }

    return { success: true };
  }
}
