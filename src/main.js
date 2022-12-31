/*
    Fails Components (Fancy Automated Internet Lecture System - Components)
    Copyright (C)  2015-2017 (original FAILS), 
                   2021- (FAILS Components)  Marten Richter <marten.richter@freenet.de>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

import * as redis from 'redis'
import MongoClient from 'mongodb'
import { Housekeeping } from './housekeeper.js'
import { writeFile, rm } from 'node:fs/promises'
import { FailsConfig } from '@fails-components/config'
import { FailsAssets } from '@fails-components/security'
import { CronJob } from 'cron'

const initApp = async () => {
  const cfg = new FailsConfig()
  let rediscl
  let redisclusterconfig
  if (cfg.getRedisClusterConfig)
    redisclusterconfig = cfg.getRedisClusterConfig()
  if (!redisclusterconfig) {
    console.log(
      'Connect to redis database with host:',
      cfg.redisHost(),
      'and port:',
      cfg.redisPort()
    )
    rediscl = redis.createClient({
      socket: { port: cfg.redisPort(), host: cfg.redisHost() },
      password: cfg.redisPass()
    })
  } else {
    // cluster case
    console.log('Connect to redis cluster with config:', redisclusterconfig)
    rediscl = redis.createCluster(redisclusterconfig)
  }

  await rediscl.connect()
  console.log('redisclient connected')

  const mongoclient = await MongoClient.connect(cfg.getMongoURL(), {
    useNewUrlParser: true,
    useUnifiedTopology: true
  })
  const mongodb = mongoclient.db(cfg.getMongoDB())

  const assets = new FailsAssets({
    datadir: cfg.getDataDir(),
    dataurl: cfg.getURL('data'),
    webservertype: cfg.getWSType(),
    savefile: cfg.getStatSaveType(),
    privateKey: cfg.getStatSecret(),
    swift: cfg.getSwift()
  })

  const hk = new Housekeeping({
    redis: rediscl,
    mongo: mongodb,
    deleteAsset: assets.shadelete,
    setupAssets: assets.setupAssets
  })

  let hklocktime = 0
  // eslint-disable-next-line no-unused-vars
  const hkjob = new CronJob(
    '35 * * * * *',
    () => {
      console.log('Start house keeping')
      if (Date.now() - hklocktime > 1000 * 40) {
        hklocktime = Date.now()
        writeFile('/tmp/healthy.txt', 'ok').catch((error) => {
          console.log('Writing health file failed.', error)
        })
        hk.houseKeeping().catch((error) => {
          console.log('Problem with housekeeping:', error)
          rm('/tmp/healthy.txt').catch((error) => {
            console.log('Removing health file failed.', error)
          })
        })
      } else {
        console.log('housekeeping blocked')
        rm('/tmp/healthy.txt').catch((error) => {
          console.log('Removing health file failed.', error)
        })
      }
      console.log('End house keeping')
    },
    null,
    true
  ) // run it every minute
}
initApp()
