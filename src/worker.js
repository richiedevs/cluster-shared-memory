const cluster = require('cluster');
const { v4: uuid4 } = require('uuid');

class Worker {
	constructor() {
		/**
		 * Record the callback functions of requests.
		 * @private
		 */
		this.__getCallbacks__ = {};

		/**
		 * Record the listeners' callback functions.
		 * @private
		 */
		this.__getListenerCallbacks__ = {};

		// Listen the returned messages from master processes.
		process.on('message', data => {
			// Mark this is a share memory.
			if (!data.isSharedMemoryMessage) return;
			if (data.isNotified) {
				const callback = this.__getListenerCallbacks__[data.uuid];
				if (callback && typeof callback === 'function') {
					callback(data.value);
				}
			} else {
				const cb = this.__getCallbacks__[data.uuid];
				if (cb && typeof cb === 'function') {
					cb(data.value);
				}
				delete this.__getCallbacks__[data.uuid];
			}
		});
	}

	/**
	 * Write data.
	 * @param {object} k
	 * @param {*} v
	 * @param {function?} cb
	 */
	set(k, v, cb) {
		if (typeof cb === 'function') {
			this.handle('set', k, v, cb);
		}
		return new Promise(resolve => {
			this.handle('set', k, v, () => {
				resolve();
			});
		});
	}

	/**
	 * Read data.
	 * @param {object} k
	 * @param {function?} cb
	 * @returns {*}
	 */
	get(k, cb) {
		if (typeof cb === 'function') {
			this.handle('get', k, null, cb);
		}
		return new Promise(resolve => {
			this.handle('get', k, null, v => {
				resolve(v);
			});
		});
	}

	/**
	 * Remove data.
	 * @param {object} k
	 * @param {function?} cb
	 */
	remove(k, cb) {
		if (typeof cb === 'function') {
			this.handle('remove', k, null, cb);
		}
		return new Promise(resolve => {
			this.handle('remove', k, null, () => {
				resolve();
			});
		});
	}

	/**
	 * Get the Lock of an object.
	 * @param {object} k
	 * @param {function?} cb
	 * @returns {*}
	 */
	getLock(k, cb) {
		if (typeof cb === 'function') {
			this.handle('getLock', k, null, cb);
		}
		return new Promise(resolve => {
			this.handle('getLock', k, null, v => {
				resolve(v);
			});
		});
	}

	/**
	 * Release the Lock of an object.
	 * @param {object} k
	 * @param {string} lockId
	 * @param {function?} cb
	 * @returns {*}
	 */
	releaseLock(k, lockId, cb) {
		if (typeof cb === 'function') {
			this.handle('releaseLock', k, lockId, cb);
		}
		return new Promise(resolve => {
			this.handle('releaseLock', k, lockId, v => {
				resolve(v);
			});
		});
	}

	/**
	 * Auto get and release the Lock of an object.
	 * @param {object} k
	 * @param {function?} f
	 * @returns {*}
	 */
	mutex(k, f) {
		return (async () => {
			const lockId = await this.getLock(k);
			const result = await f();
			await this.releaseLock(k, lockId);
			return result;
		})();
	}

	/**
	 * Send the requests to the master process.
	 * @private
	 * @param {string} [method=set|get]
	 * @param {object} k
	 * @param {*} v
	 * @param {function(data)} [cb] - the callback function
	 */
	handle(method, k, v, cb) {
		const uuid = uuid4(); // communication ID
		process.send({
			isSharedMemoryMessage: true,
			id: cluster.worker.id,
			method,
			uuid,
			key: k,
			value: v,
		});
		if (method === 'listen') {
			this.__getListenerCallbacks__[uuid] = cb;
		} else {
			this.__getCallbacks__[uuid] = cb;
		}
	}

	/**
	 * Listen an object.
	 * @param {object} k
	 * @param {function} cb
	 * @returns {*}
	 */
	listen(k, cb) {
		if (typeof cb === 'function') {
			this.handle('listen', k, null, cb);
		} else {
			throw new Error('a listener must have a callback.');
		}
	}

	/**
	 * Read the LRU shared memory.
	 * @param {object} k
	 * @param {function?} cb
	 */
	getLRU(k, cb) {
		if (typeof cb === 'function') {
			this.handle('getLRU', k, null, cb);
		}
		return new Promise(resolve => {
			this.handle('getLRU', k, null, (value) => {
				resolve(value);
			});
		});
	}

	/**
	 * Write the LRU shared memory.
	 * @param {object} k
	 * @param {*} v
	 * @param {function?} cb
	 */
	setLRU(k, v, cb) {
		if (typeof cb === 'function') {
			this.handle('setLRU', k, v, cb);
		}
		return new Promise(resolve => {
			this.handle('setLRU', k, v, () => {
				resolve();
			});
		});
	}

	/**
	 * Remove an object from the LRU shared memory.
	 * @param {object} key
	 * @param {function?} callback
	 */
	removeLRU(key, callback) {
		if (typeof callback === 'function') {
			this.handle('removeLRU', key, null, callback);
		}
		return new Promise(resolve => {
			this.handle('removeLRU', key, null, () => {
				resolve();
			});
		});
	}
}

module.exports = Worker;
