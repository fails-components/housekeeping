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

import { promisify } from 'util'
import Redlock from 'redlock'

export class Housekeeping {
  constructor(args) {
    this.redis = args.redis
    this.mongo = args.mongo

    this.deleteAsset = args.deleteAsset

    this.redlock = new Redlock([this.redis], {
      driftFactor: 0.01, // multiplied by lock ttl to determine drift time

      retryCount: 10,

      retryDelay: 200, // time in ms
      retryJitter: 200 // time in ms
    })

    // this.lastaccess = this.lastaccess.bind(this)
  }

  async houseKeeping() {
    let lock
    try {
      lock = await this.redlock.lock('housekeeping', 2000)
      console.log('Do saveChangedLectures')
      await this.saveChangedLectures()
      console.log('tryLectureRedisPurge')
      await this.tryLectureRedisPurge()
      console.log('delete orphaned lectures')
      await this.deleteOrphanedLect()
      console.log('delete orphaned lectures done')
      console.log('check for assets to delete')
      await this.checkAssetsforDelete()
      console.log('check for assets to delete done')
      console.log('House keeping done!')
      lock.unlock()
    } catch (error) {
      console.log('Busy or Error in Housekeeping', error)
    }
  }

  async saveChangedLectures() {
    const client = this.redis
    const scan = promisify(this.redis.scan).bind(client)
    const hmget = promisify(this.redis.hmget).bind(client)
    try {
      let cursor = 0
      do {
        const scanret = await scan(
          cursor,
          'MATCH',
          'lecture:????????-????-????-????-????????????',
          'COUNT',
          20
        )
        // console.log("scanret", scanret);
        // got the lectures now figure out, which we need to save
        const saveproms = Promise.all(
          scanret[1].map(async (el) => {
            const info = await hmget(el, 'lastwrite', 'lastDBsave')
            // console.log("our info",info);
            if (info[0] > info[1] + 3 * 60 * 1000) {
              // do not save more often than every 3 minutes
              const lectureuuid = el.substr(8)
              return this.saveLectureToDB(lectureuuid)
            } else return null
          })
        )
        await saveproms // wait before next iteration, do not use up to much mem

        cursor = scanret[0]
      } while (cursor !== '0')
    } catch (error) {
      console.log('Error saveChangedLecture', error)
    }
  }

  async saveLectureToDB(lectureuuid) {
    const client = this.redis
    const smembers = promisify(this.redis.smembers).bind(client)
    const get = promisify(this.redis.get).bind(client)
    const hset = promisify(this.redis.hset).bind(client)
    const hget = promisify(this.redis.hget).bind(client)
    const time = Date.now()
    console.log('Try saveLectureToDB  for lecture', lectureuuid)
    try {
      const lecturescol = this.mongo.collection('lectures')
      const boardscol = this.mongo.collection('lectureboards')
      // we got now through all boards and save them to the db

      const boardprefix = 'lecture:' + lectureuuid + ':board'
      let update = []
      const backgroundp = hget('lecture:' + lectureuuid, 'backgroundbw')

      const members = await smembers(boardprefix + 's')
      const copyprom = Promise.all(
        members.map(async (el) => {
          const boardname = el
          // if (boardname=="s") return null; // "boards excluded"
          // console.log("one board", el);
          // console.log("boardname", boardname);
          const boarddata = await get(Buffer.from(boardprefix + el))
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
      await hset('lecture:' + lectureuuid, 'lastDBsave', Date.now())
      console.log('saveLectureToDB successful for lecture', lectureuuid)
    } catch (err) {
      console.log('saveLectToDBErr', err, lectureuuid)
    }
  }

  async tryLectureRedisPurge() {
    const client = this.redis
    // ok we got through all lectures and collect last access times
    const scan = promisify(this.redis.scan).bind(client)
    const hmget = promisify(this.redis.hmget).bind(client)
    const unlink = promisify(this.redis.unlink).bind(client)

    try {
      let cursor = 0
      const allprom = []

      do {
        const scanret = await scan(
          cursor,
          'MATCH',
          'lecture:????????-????-????-????-????????????',
          'COUNT',
          40
        )
        // ok we figure out one by one if we should delete
        // console.log("purge scanret", scanret);
        const myprom = Promise.all(
          scanret[1].map(async (el) => {
            const lastaccessesp = []

            lastaccessesp.push(hmget(el, 'lastwrite', 'lastaccess'))

            // ok but also the notescreens are of interest

            let cursor2 = 0
            do {
              const scanret2 = await scan(
                cursor2,
                'MATCH',
                el + ':notescreen:????????-????-????-????-????????????'
              )
              // console.log("purge scanret2", scanret2);
              const myprom2 = scanret2[1].map((el2) => {
                return hmget(el2, 'lastaccess')
              })
              lastaccessesp.push(...myprom2)

              cursor2 = scanret2[0]
            } while (cursor2 !== '0')

            let laarr = await Promise.all(lastaccessesp)
            laarr = laarr.flat()
            // console.log("laar",laarr);
            const la = Math.max(...laarr)
            // console.log("lastaccess",la,Date.now()-la );
            const retprom = []
            // console.log("before purge");
            if (Date.now() - la > 30 * 60 * 1000) {
              console.log('Starting to purge lecture ', el)
              // purge allowed
              retprom.push(unlink(el))
              let pcursor = 0
              do {
                const pscanret = await scan(pcursor, 'MATCH', el + ':*')
                console.log('purge element', pscanret)
                pcursor = pscanret[0]
                retprom.push(...pscanret[1].map((el2) => unlink(el2)))
              } while (pcursor !== '0')
            }
            return Promise.all(retprom)
          })
        )
        allprom.push(myprom)
        cursor = scanret[0]
      } while (cursor !== '0')
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

      const client = this.redis
      const sadd = promisify(this.redis.sadd).bind(client)

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

      let deletedoc = await lecturescol.findOneAndDelete(query, {
        projection: { _id: 0, usedpictures: 1, pictures: 1 }
      })
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

        if (deletedoc.backgroundpdf) {
          const pdf = [deletedoc.backgroundpdf.sha]
          deleteprom.push(sadd('checkdel:pdf', pdf.toString('hex')))
        }

        if (jpg.length > 0) deleteprom.push(sadd('checkdel:jpg', jpg))
        if (png.length > 0) deleteprom.push(sadd('checkdel:png', png))
        if (tjpg.length > 0) deleteprom.push(sadd('checkdel:jpg', tjpg))
        if (tpng.length > 0) deleteprom.push(sadd('checkdel:png', tpng))
        // console.log('delete doc', deletedoc)

        deletedoc = await nextdeletedoc
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
  }

  async checkAssetsforDeleteInt(fileext) {
    const client = this.redis
    const count = 10
    const spop = promisify(this.redis.spop).bind(client)
    try {
      const lecturescol = this.mongo.collection('lectures')
      let curset = await spop('checkdel:' + fileext, count)

      while (curset.length > 0) {
        const nextset = spop('checkdel:' + fileext, count)
        const myprom = curset.map(async (el) => {
          const query = {
            $or: [
              { 'backgroundpdf.sha': Buffer.from(el, 'hex') },
              { 'usedpictures.sha': Buffer.from(el, 'hex') },
              { 'pictures.sha': Buffer.from(el, 'hex') },
              { 'usedpictures.tsha': Buffer.from(el, 'hex') },
              { 'pictures.tsha': Buffer.from(el, 'hex') }
            ]
          }
          const res = await lecturescol.findOne(query, {
            projection: { _id: 0 }
          })
          if (!res) {
            await this.deleteAssetfile(el, fileext)
          }
        })
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
}