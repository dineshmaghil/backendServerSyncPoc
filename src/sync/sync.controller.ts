import { Controller, Get, Post, Query, Body } from '@nestjs/common';
import { SyncService } from './sync.service';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Get()
  async pull(@Query('last_pulled_at') lastPulledAt: string) {
    return this.syncService.pull(lastPulledAt);
  }

  @Post()
  async push(@Body() changes: any, @Query('last_pulled_at') lastPulledAt: string) {
    return this.syncService.push(changes, lastPulledAt);
  }
}

