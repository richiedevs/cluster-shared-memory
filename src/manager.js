const cluster = require('cluster');
const { v4: uuid4 } = require('uuid');
const { LRUCache } = require('lru-cache');

class Manager {
	constructor() {
		// The shared memory is managed by the master process.
		this.__sharedMemory__ = {
			set(k, v) {
				this.memory[k] = v;
			},
			get(k) {
				return this.memory[k];
			},
			clear() {
				this.memory = {};
			},
			remove(k) {
				delete this.memory[k];
			},
			memory: {},
			locks: {},
			lockRequestQueues: {},
			listeners: {},
		};

		this.defaultLRUOptions = { max: 10000, maxAge: 5 * 60 * 1000 };

		this.__sharedLRUMemory__ = new LRUCache(this.defaultLRUOptions);

		this.logger = console.error;

		// Listen the messages from worker processes.
		cluster.on('online', worker => {
			worker.on('message', data => {
				if (!data.isSharedMemoryMessage) return;
				this.handle(data, worker);
				return false;
			});
		});
	}

	/**
	 * recreate LRU cache
	 * @param options
	 */
	setLRUOptions(options) {
		this.__sharedLRUMemory__ = new LRUCache({
			...this.defaultLRUOptions,
			...options
		});
	}

	/**
	 * handle the requests from worker processes.
	 * @private
	 * @param {object} data
	 * @param {cluster.Worker} target
	 */
	handle(data, target) {
		if (data.method === 'listen') {
			const listener = (value) => {
				if (target.isConnected()) {
					const msg = {
						isSharedMemoryMessage: true,
						isNotified: true,
						id: data.id, // worker id
						uuid: data.uuid,
						value,
					};
					target.send(msg, null, (error) => {
						if (error) {
							// Usually happens when the worker exit before the data is sent.
							this.logger(error);
						}
					});
				} else if (target.isDead()) {
					this.removeListener(data.key, listener);
				}
			};
			this.listen(data.key, listener);
		} else {
			const args = data.value ? [data.key, data.value] : [data.key];
			this[data.method](...args).then((value) => {
				const msg = {
					isSharedMemoryMessage: true,
					isNotified: false,
					id: data.id, // worker id
					uuid: data.uuid,
					value,
				};
				target.send(msg, null, e => {
					if (e) {
						// Usually happens when the worker exit before the data is sent.
						this.logger(e);
					}
				});
			});
		}
	}

	/**
	 * @private
	 */
	notifyListener(k) {
		const listeners = this.__sharedMemory__.listeners[k];
		if (listeners?.length > 0) {
			Promise.all(
				listeners.map(
					cb =>
						new Promise(resolve => {
							cb(this.__sharedMemory__.get(k));
							resolve();
						})
				)
			);
		}
	}

	/**
	 * Write the shared memory.
	 * @param {object} k
	 * @param {*} v
	 * @param {function?} cb
	 */
	set(k, v, cb) {
		if (k) {
			if (typeof cb === 'function') {
				this.__sharedMemory__.set(k, v);
				this.notifyListener(k);
				cb('OK');
			}
			return new Promise(resolve => {
				this.__sharedMemory__.set(k, v);
				this.notifyListener(k);
				resolve('OK');
			});
		}
	}

	/**
	 * Read the shared memory.
	 * @param {object} k
	 * @param {function?} cb
	 */
	get(k, cb) {
		if (typeof cb === 'function') {
			cb(this.__sharedMemory__.get(k));
		}
		return new Promise(resolve => {
			resolve(this.__sharedMemory__.get(k));
		});
	}

	/**
	 * Remove an object from the shared memory.
	 * @param {object} k
	 * @param {function?} cb
	 */
	remove(k, cb) {
		if (typeof cb === 'function') {
			this.__sharedMemory__.remove(k);
			this.notifyListener(k);
			cb('OK');
		}
		return new Promise(resolve => {
			this.__sharedMemory__.remove(k);
			this.notifyListener(k);
			resolve('OK');
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
			this.__sharedMemory__.lockRequestQueues[k] =
				this.__sharedMemory__.lockRequestQueues[k] ?? [];
			this.__sharedMemory__.lockRequestQueues[k].push(cb);
			this.handleLockRequest(k);
		}
		return new Promise(resolve => {
			this.__sharedMemory__.lockRequestQueues[k] =
				this.__sharedMemory__.lockRequestQueues[k] ?? [];
			this.__sharedMemory__.lockRequestQueues[k].pushresolve;
			this.handleLockRequest(k);
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
			if (lockId === this.__sharedMemory__.locks[k]) {
				delete this.__sharedMemory__.locks[k];
				this.handleLockRequest(k);
				cb('OK');
			} else {
				cb(`Failed. LockId:${lockId} does not match ${k}'s lockId.`);
			}
		}
		return new Promise(resolve => {
			if (lockId === this.__sharedMemory__.locks[k]) {
				delete this.__sharedMemory__.locks[k];
				this.handleLockRequest(k);
				resolve('OK');
			} else {
				resolve(`Failed. LockId:${lockId} does not match ${k}'s lockId.`);
			}
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
	 * @private
	 */
	handleLockRequest(k) {
		return new Promise(resolve => {
			if (
				!this.__sharedMemory__.locks[k] &&
				this.__sharedMemory__.lockRequestQueues[k]?.length > 0
			) {
				const callback = this.__sharedMemory__.lockRequestQueues[k].shift();
				const lockId = uuid4();
				this.__sharedMemory__.locks[k] = lockId;
				callback(lockId);
			}
			resolve();
		});
	}

	/**
	 * Listen an object.
	 * @param {object} k
	 * @param {function} cb
	 * @returns {*}
	 */
	listen(k, cb) {
		if (typeof cb === 'function') {
			this.__sharedMemory__.listeners[k] =
				this.__sharedMemory__.listeners[k] ?? [];
			this.__sharedMemory__.listeners[k].push(cb);
		} else {
			throw new Error('a listener must have a callback.');
		}
	}

	/**
	 * Remove a listener of an object.
	 * @private
	 * @param {object} k
	 * @param {function} listener
	 */
	removeListener(k, listener) {
		const index = (this.__sharedMemory__.listeners[k] ?? []).indexOf(listener);
		if (index >= 0) {
			this.__sharedMemory__.listeners[k].splice(index, 1);
		}
	}

	/**
	 * Read the LRU shared memory.
	 * @param {object} k
	 * @param {function?} cb
	 */
	getLRU(k, cb) {
		if (typeof cb === 'function') {
			cb(this.__sharedLRUMemory__.get(k));
		}
		return new Promise(resolve => {
			resolve(this.__sharedLRUMemory__.get(k));
		});
	}

	/**
	 * Write the LRU shared memory.
	 * @param {object} k
	 * @param {*} v
	 * @param {function?} cb
	 */
	setLRU(k, v, cb) {
		if (k) {
			if (typeof cb === 'function') {
				this.__sharedLRUMemory__.set(k, v);
				cb('OK');
			}
			return new Promise(resolve => {
				this.__sharedLRUMemory__.set(k, v);
				resolve('OK');
			});
		}
	}

	/**
	 * Remove an object from the LRU shared memory.
	 * @param {object} k
	 * @param {function?} cb
	 */
	removeLRU(k, cb) {
		if (typeof cb === 'function') {
			this.__sharedLRUMemory__.del(k);
			cb('OK');
		}
		return new Promise(resolve => {
			this.__sharedLRUMemory__.del(k);
			resolve('OK');
		});
	}
}

module.exports = Manager;
