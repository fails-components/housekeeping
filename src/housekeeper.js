/* eslint-disable node/no-callback-literal */
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

import Redlock from 'redlock'
import { RedisRedlockProxy } from '@fails-components/security'
import { commandOptions } from 'redis'

export class Housekeeping {
  constructor(args) {
    this.redis = args.redis
    this.mongo = args.mongo

    this.deleteAsset = args.deleteAsset
    this.setupAssets = args.setupAssets

    this.redlock = new Redlock([RedisRedlockProxy(this.redis)], {
      driftFactor: 0.01, // multiplied by lock ttl to determine drift time

      retryCount: 10,

      retryDelay: 200, // time in ms
      retryJitter: 200 // time in ms
    })

    // this.lastaccess = this.lastaccess.bind(this)

    this.createMongoIndices().catch((error) => {
      console.log('problem creatung mongo indices', error)
    })
    this.setupAssets().catch((error) => {
      console.log('problem settign up assests', error)
    })

    this.lastInformAVS = { localAlarm: undefined, globalAlarm: undefined }
    this.mailtransport = args.mailtransport
    this.senderaddress = args.senderaddress
    this.rootemails = args.rootemails
  }

  async createMongoIndices() {
    // note: if an indices changes, that was released, it will be delete here and recreated

    // create indices,
    try {
      const lecturescol = this.mongo.collection('lectures')

      // first lectures
      // first one the unique identifier
      const uuidres = await lecturescol.createIndex(
        { uuid: 1 },
        { unique: true }
      ) // this one is unique
      console.log('lectures unique uuid index create', uuidres)

      const lmsres = await lecturescol.createIndex({
        'lms.iss': 1,
        'lms.resource_id': 1
      })
      console.log('lectures lms index create', lmsres)

      const ownersres = await lecturescol.createIndex({ owners: 1 })
      console.log('lectures owners index create', ownersres)

      // second boards
      const boardscol = this.mongo.collection('lectureboards')
      const buuidres = await boardscol.createIndex({ uuid: 1, board: 1 }) // this one is not unique, currently board is not needed but may be useful in the future
      console.log('boards uuid and board name index create', buuidres)

      const userscol = this.mongo.collection('users')
      const uuuidres = await userscol.createIndex({ uuid: 1 }, { unique: true }) // this one is unique
      console.log('users unique uuid index create', uuuidres)
      const usernameres = await userscol.createIndex(
        { 'lms.username': 1 },
        { unique: true }
      ) // this one is unique
      console.log('users unique username index create', usernameres)

      const emailres = await userscol.createIndex(
        { email: 1 },
        { unique: true }
      ) // this one is unique
      console.log('users unique email index create', emailres)

      const avsroutercol = this.mongo.collection('avsrouters')

      const expres = await avsroutercol.createIndex(
        { changedAt: 1 },
        { expireAfterSeconds: 45 }
      )
      console.log('avsrouter expire index create', expres)
      const urlres = await avsroutercol.createIndex(
        { url: 1 },
        { unique: true }
      ) // this one is unique
      console.log('avsrouter unique url index create', urlres)

      const clientres = await avsroutercol.createIndex({ clients: 1 }) // this one is unique
      console.log('avsrouter client index create', clientres)

      const primaryRealmsRes = await avsroutercol.createIndex({
        region: 1,
        primaryRealms: 1
      }) // this one is unique
      console.log('avsrouter primaryRealm index create', primaryRealmsRes)

      const localPrimaryRealmsRes = await avsroutercol.createIndex({
        region: 1,
        localPrimaryRealms: 1
      }) // this one is unique
      console.log(
        'avsrouter localPrimaryRealm index create',
        localPrimaryRealmsRes
      )

      const avsregioncol = this.mongo.collection('avsregion')

      const expregres = await avsregioncol.createIndex(
        { changedAt: 1 },
        { expireAfterSeconds: 60 * 60 * 24 }
      )
      console.log('avsregion expire index create', expregres)
      const regnameres = await avsregioncol.createIndex(
        { name: 1 },
        { unique: true }
      ) // this one is unique
      console.log('avsregion unique name index create', regnameres)
    } catch (error) {
      console.log('problem with mongodb index creation', error)
    }
  }

  async houseKeeping() {
    let lock
    try {
      lock = await this.redlock.lock('housekeeping', 2000)
      console.log('Do saveChangedLectures ' + new Date().toLocaleString())
      await this.saveChangedLectures()
      console.log('tryLectureRedisPurge ' + new Date().toLocaleString())
      await this.tryLectureRedisPurge()
      console.log('delete orphaned lectures ' + new Date().toLocaleString())
      await this.deleteOrphanedLect()
      console.log(
        'delete orphaned lectures done ' + new Date().toLocaleString()
      )
      console.log('check for assets to delete ' + new Date().toLocaleString())
      await this.checkAssetsforDelete()
      console.log(
        'check for assets to delete done ' + new Date().toLocaleString()
      )
      console.log('House keeping done! ' + new Date().toLocaleString())
      lock.unlock()
    } catch (error) {
      console.log('Busy or Error in Housekeeping', error)
    }
  }

  async saveChangedLectures() {
    try {
      let cursor = 0
      do {
        const scanret = await this.redis.scan(cursor, {
          MATCH: 'lecture:????????-????-????-????-????????????',
          COUNT: 20
        })
        // got the lectures now figure out, which we need to save
        const saveproms = Promise.all(
          scanret.keys.map(async (el) => {
            const info = await this.redis.hmGet(el, ['lastwrite', 'lastDBsave'])
            if (
              Number(info[0]) > Number(info[1]) + 3 * 60 * 1000 ||
              (Date.now() > Number(info[1]) + 5 * 60 * 1000 &&
                Number(info[0]) > Number(info[1]))
            ) {
              // do not save more often than every 3 minutes
              // or you have waited for five minutes
              const lectureuuid = el.substr(8)
              return this.saveLectureToDB(lectureuuid)
            } else return null
          })
        )
        await saveproms // wait before next iteration, do not use up to much mem

        cursor = scanret.cursor
      } while (cursor !== 0)
    } catch (error) {
      console.log('Error saveChangedLecture', error)
    }
  }

  async saveLectureToDB(lectureuuid) {
    const time = Date.now()
    console.log('Try saveLectureToDB  for lecture', lectureuuid)
    try {
      const lecturescol = this.mongo.collection('lectures')
      const boardscol = this.mongo.collection('lectureboards')
      // we got now through all boards and save them to the db

      const boardprefix = 'lecture:' + lectureuuid + ':board'
      let update = []
      const backgroundp = this.redis.hGet(
        'lecture:' + lectureuuid,
        'backgroundbw'
      )

      const members = await this.redis.sMembers(boardprefix + 's')
      const copyprom = Promise.all(
        members.map(async (el) => {
          const boardname = el ? el.toString() : null
          // if (boardname=="s") return null; // "boards excluded"
          // console.log("one board", el);
          // console.log("boardname", boardname);
          let boarddata
          if (this.redis.getBuffer)
            // required for v 4.0.0, remove later
            boarddata = await this.redis.getBuffer(boardprefix + boardname)
          // future api
          else
            boarddata = await this.redis.get(
              commandOptions({ returnBuffers: true }),
              boardprefix + boardname
            )

          if (boarddata) {
            // got it now store it
            update = boardscol.updateOne(
              { uuid: lectureuuid, board: boardname },
              {
                $set: {
                  savetime: time,
                  boarddata: boarddata
                }
              },
              { upsert: true }
            )
            return Promise.all([boardname, update])
          } else return null
        })
      )
      update = update.concat(await copyprom) // reduces memory footprint

      const allboards = update
        .filter((el) => !!el)
        .map((el) => (el ? el[0] : null))
      // console.log("allbaords", allboards);
      const backgroundbw = await backgroundp
      lecturescol.updateOne(
        { uuid: lectureuuid },
        {
          $set: {
            boards: allboards,
            boardsavetime: time,
            backgroundbw: backgroundbw
          }
        }
      )
      await this.redis.hSet('lecture:' + lectureuuid, [
        'lastDBsave',
        Date.now().toString()
      ])
      console.log('saveLectureToDB successful for lecture', lectureuuid)
    } catch (err) {
      console.log('saveLectToDBErr', err, lectureuuid)
    }
  }

  async tryLectureRedisPurge() {
    try {
      let cursor = 0
      const allprom = []

      do {
        const scanret = await this.redis.scan(cursor, {
          MATCH: 'lecture:????????-????-????-????-????????????',
          COUNT: 40
        })
        // ok we figure out one by one if we should delete
        // console.log('purge scanret', scanret)
        const myprom = Promise.all(
          scanret.keys.map(async (el) => {
            const lastaccessesp = []

            lastaccessesp.push(this.redis.hmGet(el, 'lastwrite', 'lastaccess'))

            // ok but also the notescreens are of interest

            let cursor2 = 0
            do {
              const scanret2 = await this.redis.scan(cursor2, {
                MATCH: el + ':notescreen:????????-????-????-????-????????????'
              })
              // console.log('purge scanret2', scanret2)
              const myprom2 = scanret2.keys.map((el2) => {
                return this.redis.hmGet(el2, 'lastaccess')
              })
              lastaccessesp.push(...myprom2)

              cursor2 = scanret2.cursor
            } while (cursor2 !== 0)

            let laarr = await Promise.all(lastaccessesp)
            laarr = laarr.flat().map((el) => Number(el))
            // console.log("laar",laarr);
            const la = Math.max(...laarr)
            // console.log("lastaccess",la,Date.now()-la );
            const retprom = []
            // console.log("before purge");
            if (Date.now() - Number(la) > 30 * 60 * 1000) {
              console.log('Starting to purge lecture ', el)
              // purge allowed
              retprom.push(this.redis.unlink(el))
              retprom.push(this.redis.sRem('lectures', el))
              let pcursor = 0
              do {
                const pscanret = await this.redis.scan(pcursor, {
                  MATCH: el + ':*'
                })
                console.log('purge element', pscanret)
                pcursor = pscanret.cursor
                retprom.push(
                  ...pscanret.keys.map((el2) => this.redis.unlink(el2), this)
                )
              } while (pcursor !== 0)
            }
            return Promise.all(retprom)
          }, this)
        )
        allprom.push(myprom)
        cursor = scanret.cursor
      } while (cursor !== 0)
      await Promise.all(allprom) // we are finished giving orders, wait for return
      return
    } catch (err) {
      console.log('tryLectureRedisPurge error', err)
    }
  }

  async deleteOrphanedLect() {
    try {
      const lecturescol = this.mongo.collection('lectures')
      const boardscol = this.mongo.collection('lectureboards')

      // first find all lectures that are orphaned, that means no course and no owner
      const query = {
        $and: [
          {
            'lms.resource_id': { $exists: false }
          },
          {
            $or: [
              { owners: { $exists: false } },
              { owners: { $exists: true, $size: 0 } }
            ]
          }
        ]
      }

      let deletedoc = (
        await lecturescol.findOneAndDelete(query, {
          projection: {
            _id: 0,
            usedpictures: 1,
            pictures: 1,
            backgroundpdf: 1,
            usedipynbs: 1,
            ipynbs: 1
          }
        })
      ).value
      const deleteprom = []
      while (deletedoc != null) {
        const nextdeletedoc = lecturescol.findOneAndDelete(query)
        // do sth
        const lectureuuid = deletedoc.uuid
        // purge all connected boards
        deleteprom.push(boardscol.deleteMany({ uuid: lectureuuid }))
        // put all connected boards to a set for potential deletion
        let pictset = []
        if (deletedoc.usedpictures)
          pictset = pictset.concat(deletedoc.usedpictures)
        if (deletedoc.pictures) pictset = pictset.concat(deletedoc.pictures)

        let jpg = pictset
          .filter((el) => el.mimetype === 'image/jpeg')
          .map((el) => el.sha.toString('hex'))
        let png = pictset
          .filter((el) => el.mimetype === 'image/png')
          .map((el) => el.sha.toString('hex'))

        let tjpg = pictset
          .filter((el) => el.mimetype === 'image/jpeg')
          .map((el) => el.tsha.toString('hex'))
        let tpng = pictset
          .filter((el) => el.mimetype === 'image/png')
          .map((el) => el.tsha.toString('hex'))

        jpg = [...new Set(jpg)]
        png = [...new Set(png)]
        tjpg = [...new Set(tjpg)]
        tpng = [...new Set(tpng)]

        let ipynbset = []
        if (deletedoc.usedipynbs)
          ipynbset = ipynbset.concat(deletedoc.usedipynbs)
        if (deletedoc.ipynbs) ipynbset = ipynbset.concat(deletedoc.ipynbs)

        let ipynb = ipynbset.map((el) => el.sha.toString('hex'))

        ipynb = [...new Set(ipynb)]

        if (ipynb.length > 0)
          deleteprom.push(this.redis.sAdd('checkdel:ipynb', ipynb))

        if (deletedoc.backgroundpdf) {
          const pdf = [deletedoc.backgroundpdf.sha.toString('hex')]
          deleteprom.push(this.redis.sAdd('checkdel:pdf', pdf))
        }

        if (jpg.length > 0)
          deleteprom.push(this.redis.sAdd('checkdel:jpg', jpg))
        if (png.length > 0)
          deleteprom.push(this.redis.sAdd('checkdel:png', png))
        if (tjpg.length > 0)
          deleteprom.push(this.redis.sAdd('checkdel:jpg', tjpg))
        if (tpng.length > 0)
          deleteprom.push(this.redis.sAdd('checkdel:png', tpng))
        // console.log('delete doc', deletedoc)

        deletedoc = (await nextdeletedoc).value
      }
      await Promise.all(deleteprom) // wait that we are ready before doing other stuff
    } catch (error) {
      console.log('error in delete Orphaned lectures', error)
    }
  }

  async checkAssetsforDelete() {
    await this.checkAssetsforDeleteInt('jpg')
    await this.checkAssetsforDeleteInt('png')
    await this.checkAssetsforDeleteInt('pdf')
    await this.checkAssetsforDeleteInt('ipynb')
  }

  async checkAssetsforDeleteInt(fileext) {
    const count = 10
    try {
      const lecturescol = this.mongo.collection('lectures')
      let curset = await this.redis.sPop('checkdel:' + fileext, count)

      while (curset.length > 0) {
        const nextset = this.redis.sPop('checkdel:' + fileext, count)
        const myprom = curset.map(async (el) => {
          const query = {
            $or: [
              { 'backgroundpdf.sha': Buffer.from(el, 'hex') },
              { 'usedpictures.sha': Buffer.from(el, 'hex') },
              { 'pictures.sha': Buffer.from(el, 'hex') },
              { 'usedpictures.tsha': Buffer.from(el, 'hex') },
              { 'pictures.tsha': Buffer.from(el, 'hex') },
              { 'usedipynbs.sha': Buffer.from(el, 'hex') },
              { 'ipynbs.sha': Buffer.from(el, 'hex') }
            ]
          }
          const res = await lecturescol.findOne(query, {
            projection: { _id: 0 }
          })
          if (!res) {
            await this.deleteAssetfile(el, fileext)
          }
        }, this)
        await Promise.all(myprom) // keep the fs load low

        curset = await nextset
      }
    } catch (error) {
      console.log('problem in checkassetsfordelete', error)
    }
  }

  deleteAssetfile(shastr, fileext) {
    console.log('Delete asset with sha and fileext ', shastr, fileext)
    try {
      this.deleteAsset(shastr, fileext)
    } catch (error) {
      console.log(
        'Problem delete asset with sha and fileext ',
        shastr,
        fileext,
        error
      )
    }
  }

  async sendStatusInfo(attn, message) {
    console.log('Logging status info\n', attn, '\n', message)
    if (this.mailtransport && this.rootemails) {
      try {
        await this.mailtransport.sendMail({
          from: '"Fails housekeeping" <' + this.senderaddress + '>',
          to: this.rootemails.join(','),
          subject: attn,
          text: message
        })
      } catch (err) {
        console.log('Error while sending mail', err)
      }
    }
  }

  async checkCloudStatus() {
    await this.checkAVSRouterStatus()
  }

  async checkAVSRouterStatus() {
    try {
      const routercol = this.mongo.collection('avsrouters')

      const cursor = routercol.find(
        {},
        {
          projection: {
            _id: 0,
            url: 1,
            region: 1,
            numClients: 1,
            maxClients: 1,
            localClients: 1,
            remoteClients: 1,
            primaryRealms: 1 // Realm is lecture id
          }
        }
      )

      const routers = []
      while (await cursor.hasNext()) {
        const {
          region,
          localClients,
          remoteClients,
          numClients,
          maxClients,
          primaryRealms,
          url
        } = await cursor.next()

        routers.push({
          url,
          region,
          numClients: numClients ?? 0,
          maxClients: maxClients ?? 0,
          numLocalClients: localClients?.length ?? 0,
          numRemoteClients: remoteClients?.length ?? 0,
          primaryLectureNum: primaryRealms?.length ?? 0
        })
      }
      // now we calculate the subscription rate
      const sumNumClients = routers.reduce(
        (prevVal, el) => prevVal + el.numClients,
        0
      )
      const sumMaxClients = routers.reduce(
        (prevVal, el) => prevVal + el.maxClients,
        0
      )
      const usedRatio = sumNumClients / sumMaxClients

      const globalAlarm = usedRatio > 0.8 // make me configurable

      const regions = {}
      routers.forEach((router) => {
        if (typeof router.region === 'undefined') return
        if (!regions[router.region]) regions[router.region] = []
        const region = regions[router.region]
        region.region = router.region
        region.numClients = (region.numClients || 0) + router.numClients
        region.maxClients = (region.maxClients || 0) + router.maxClients
      })
      let localAlarm = false
      Object.values(regions).forEach((region) => {
        const usedRatio = region.numClients / region.maxClients
        region.usedRatio = usedRatio
        if (usedRatio > 0.8) localAlarm = true
      })

      if (
        this.lastInformAVS.globalAlarm !== globalAlarm ||
        this.lastInformAVS.localAlarm !== localAlarm
      ) {
        this.lastInformAVS.globalAlarm = globalAlarm
        this.lastInformAVS.localAlarm = localAlarm
        const statusMessage =
          'Updated status information (AVS routers):\n' +
          'globalAlarm: ' +
          globalAlarm +
          ' localAlarm:' +
          localAlarm +
          '\n' +
          'Router information:\n' +
          'URL | Region | numClients| maxClients | numLocalClients | numRemoteClients | primaryLectureNum \n' +
          routers
            .map(
              (el) =>
                el.url +
                ' | ' +
                el.region +
                ' | ' +
                el.numClients +
                ' | ' +
                el.maxClients +
                ' | ' +
                el.numLocalClients +
                ' | ' +
                el.numRemoteClients +
                ' | ' +
                el.primaryLectureNum +
                '\n'
            )
            .join('') +
          'Region information:\n' +
          'Region | numClients| maxClients\n' +
          Object.values(regions)
            .map(
              (el) =>
                el.region + ' | ' + el.numClients + ' | ' + el.maxClients + '\n'
            )
            .join('') +
          "This information is sent by FAILS's housekeeping service container\n"
        await this.sendStatusInfo('AVS router status change', statusMessage)
      }
    } catch (error) {
      console.log('Problem checking AVSRouter status:', error)
    }
  }
}
