import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Token Bucket (https://en.wikipedia.org/wiki/Token_bucket)
 *
 * This thread-safe bucket should support the following methods:
 *
 * - take(n): remove n tokens from the bucket (blocks until n tokens are available and taken)
 * - set(n): set the bucket to contain n tokens (to allow "hard" rate limiting)
 * - add(n): add n tokens to the bucket (to allow "soft" rate limiting)
 * - terminate(): mark the bucket as terminated (used to communicate between threads)
 * - terminated(): return true if the bucket is terminated, false otherwise
 *
 */
class TokenBucket {
	
	private AtomicLong tokens;
	private boolean isTerminated;
	private Lock lock = new ReentrantLock();
	private Condition conditionVariable; 
	
    TokenBucket() {
    	this.tokens = new AtomicLong();
    	this.conditionVariable = lock.newCondition();
    	this.isTerminated = false;
    }

    void take(long tokens) throws InterruptedException {
    	lock.lock();
    	while(this.tokens.get() - tokens < 0) {
    		this.conditionVariable.await();
    	}
    	this.tokens.addAndGet(-tokens);
    	lock.unlock();
    }

    synchronized void terminate() {
        this.isTerminated = true;
    }

    boolean terminated() {
        return this.isTerminated;
    }

    synchronized void set(long tokens) {
    	this.tokens.set(tokens);
    }
    
    void add(long tokens) {
    	this.tokens.addAndGet(tokens);
    	lock.lock();
    	this.conditionVariable.signalAll();
    	lock.unlock();
    }
}
