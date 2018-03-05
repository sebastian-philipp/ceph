import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ConfigurationService } from './configuration.service';
import { FormatterService } from './formatter.service';
import { TcmuIscsiService } from './tcmu-iscsi.service';
import { TopLevelService } from './top-level.service';

@NgModule({
  imports: [
    CommonModule
  ],
  declarations: [],
  providers: [FormatterService, TopLevelService, TcmuIscsiService, ConfigurationService]
})
export class ServicesModule { }
