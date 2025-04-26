/*
  In DS we call two operations concurrent whe they are both unware of each other
  regardless of the physical time at which they happened.

  But how can we distinguish between concurrency and casual dependency?
  Here is an example from Kleppmann (DDIA, page 189) for a single replica
*/
class Database {
  // every value consist of an identifier
  // and a map for the current version -> value
  constructor() {
    this.values = []
  }

  storeValue(id, newVal, version) {
    const currValue = this.values.find(val => val.id === id);

    if (!currValue) {
      const newEntry = { id, versions: { 1: newVal } }
      this.values.push(newEntry);
      return { lastVersion: 1, res: newEntry }
    }

    // if no version is provided
    // then we start a new concurreny branch
    // and increase the latest version number
    const currVersions = Object.keys(currValue.versions).map(Number);
    const newVersion = Math.max(...currVersions) + 1

    if (version === undefined) {
      currValue.versions[newVersion] = newVal
      return { lastVersion: newVersion, res: currValue }
    }

    // if a version is provided, we can
    // 1.) replace the given version with the new version and the new value
    currValue.versions[newVersion] = newVal

    // 2.) overwrite any smaller version with the new value (according to DDIA, page 189)
    // NOTE: This actually makes it impossible to expose
    // casual dependencies in a general way.
    //
    // Every operation with verion v_client depends of every operation with version <= v_client
    // but e.g. in the example of ham, the dependency with milk would not show up
    // because it was already concatenated with flour and given a higher version number.
    // So a the database would need to keep previous version numbers intact. The smallest
    // version number it would need to keep is one less than the version number of the smallest
    // client version number it has given out (I think)
    delete currValue.versions[version];
    for (const currVersion of Object.keys(currValue.versions)) {
      if (currVersion < version) {
        currValue.versions[currVersion] = newVal
      }
    }
    return { lastVersion: newVersion, res: currValue }
  }
}

class Client {
  constructor(db, name) {
    this.name = name
    this.lastVersion = null
    this.currValue = []
    this.db = db
  }

  static merge(val) {
    // this is where the tricky part starts. For this simple example (shopping card)
    // we simply create a union of all entries; In general, the conflict resolution
    // is much more complex
    const allValues = Object.values(val.res.versions).flatMap(a => a);
    return [...new Set(allValues)];
  }

  add(id, val) {
    this.currValue.push(val)
    const res = this.db.storeValue(id, this.currValue, this.lastVersion !== null ? this.lastVersion : undefined);
    console.log("New DB state", JSON.stringify(res, null, 4));
    this.lastVersion = res.lastVersion
    this.currValue = Client.merge(res)
    console.log(`New Client state ${this.name}`, this.lastVersion, this.currValue);
  }
}

const replica = new Database()
const c1 = new Client(replica, "C1")
const c2 = new Client(replica, "C2")

c1.add("1", "milk")
console.log('*'.repeat(70))
c2.add("1", "eggs")
console.log('*'.repeat(70))
c1.add("1", "flour")
console.log('*'.repeat(70))
c1.add("1", "bacon")
console.log('*'.repeat(70))
c2.add("1", "ham")
