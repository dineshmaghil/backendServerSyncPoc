import { Module } from '@nestjs/common';
import { PrismaModule } from './prisma/prisma.module';
import { SyncModule } from './sync/sync.module';

@Module({
  imports: [PrismaModule, SyncModule],
})
export class AppModule {}
