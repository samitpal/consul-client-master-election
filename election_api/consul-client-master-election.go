package election_api

import (
	"log"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/hashicorp/consul/api"
)

// consul sequencer uniquely identifies a lock.
type sequencer struct {
	lockIndex uint64
	session   string
	key       string
}

type leader struct {
	isLeader           bool          // the process's view of leadership.
	seq                *sequencer    // contains the LockId, sessionID and key. This gives the real leadership state.
	stopSessionRenewCh chan struct{} // stop session renew channel.
	stopCh             chan bool     // channel used to send stop signal.
	errorRetryCount    int8          // Count of errors. We use a threshold against this counter before sending the stop signal.
}

// DoJob is what needs to be implemented by the users of this library.
type DoJob interface {
	// DoJobFunc will be called in a go routine. It takes a stop channel which is a signaling mechanism used by the caller
	// for the function to return. The other channel argument is used to indicate to the caller that the function has
	// completed processing.
	DoJobFunc(chan bool, chan bool)
}

// acquireKey tries to acquire a consul key. If successful we attain mastership.
func acquireKey(cl *api.Client, key string, ttl int, sessionName string) (string, *sequencer, bool, error) {
	session := cl.Session()
	entry := &api.SessionEntry{
		TTL:      strconv.Itoa(ttl) + "s", // ttl in seconds
		Name:     sessionName,
		Behavior: api.SessionBehaviorDelete,
	}

	id, _, err := session.Create(entry, nil)
	if err != nil {
		log.Printf("Error while creating session: %v", err)
		return "", nil, false, err
	}

	// Get a handle to the KV API
	kv := cl.KV()

	val := getHostname() + ":" + strconv.Itoa(os.Getpid()) // set value in 'hostname:pid' format
	// PUT a new KV pair
	p := &api.KVPair{
		Key:     key,
		Session: id,
		Value:   []byte(val),
	}

	success, _, err := kv.Acquire(p, nil)
	if err != nil {
		log.Printf("Error while aquiring key: %v", err)
		return "", nil, false, err
	}

	// get the sequencer
	seq, err := getSequencer(kv, key)
	if err != nil {
		log.Printf("Error while retrieving sequencer %v", err)
		return "", nil, false, err
	}
	return id, seq, success, nil
}

func removeSession(cl *api.Client, id string) {
	session := cl.Session()
	_, err := session.Destroy(id, nil)
	if err != nil {
		log.Printf("Error while destroying session: %v", err)
	}
}

func getHostname() (h string) {
	h, err := os.Hostname()
	if err != nil {
		log.Println("could not get hostname")
	}
	return h
}

// getSequencer gets the lockindex, session id and the key which constitutes the sequencer for the current lock.
func getSequencer(kv *api.KV, key string) (*sequencer, error) {
	kvPair, _, err := kv.Get(key, nil)
	if err != nil {
		log.Println("Can't get the sequencer")
		return nil, err
	}
	if kvPair == nil {
		return nil, nil
	}
	seq := sequencer{}
	seq.lockIndex = kvPair.LockIndex
	seq.session = kvPair.Session
	seq.key = kvPair.Key
	return &seq, nil
}

// MaybeAcquireLeadership function takes a consul client, KV key string, ttl (in seconds), session name, exit on lock found as well
// as a DoJob implementation. It tries to acquire a lock by associating a session to the key. If acquired, it attains mastership setting the value of
// the key to hostname:pid. The DoJobFunc function is run in a go routine. The function runs till it ends voluntarily. The function
// could be sent a stop signal via the stopCh in which case the DoJobFunc should return.
// The api leverages the TTL field of sessions. The following text from the consul.io is useful to know
//
// When creating a session, a TTL can be specified. If the TTL interval expires without being renewed, the session has expired and an invalidation is triggered.
// This type of failure detector is also known as a heartbeat failure detector. It is less scalable than the gossip based failure detector as it places an
// increased burden on the servers but may be applicable in some cases. The contract of a TTL is that it represents a lower bound for invalidation; that is,
// Consul will not expire the session before the TTL is reached, but it is allowed to delay the expiration past the TTL. The TTL is renewed on session creation,
// on session renew, and on leader failover. When a TTL is being used, clients should be aware of clock skew issues: namely, time may not progress at the same
// rate on the client as on the Consul servers. It is best to set conservative TTL values and to renew in advance of the TTL to account for network delay and time skew.
//
// The final nuance is that sessions may provide a lock-delay. This is a time duration, between 0 and 60 seconds. When a session invalidation takes place,
// Consul prevents any of the previously held locks from being re-acquired for the lock-delay interval; this is a safeguard inspired by Google's Chubby.
// The purpose of this delay is to allow the potentially still live leader to detect the invalidation and stop processing requests that may lead to inconsistent state.
// While not a bulletproof method, it does avoid the need to introduce sleep states into application logic and can help mitigate many issues.
//While the default is to use a 15 second delay, clients are able to disable this mechanism by providing a zero delay value.
func MaybeAcquireLeadership(client *api.Client, key string, ttl int, sessionName string, onLockFoundExit bool, j DoJob) {
	l := leader{}
	sleepTime := time.Duration(ttl)
	// buffered to accept if we receive the stop signal.
	doneCh := make(chan bool, 1)
	for {
		select {
		case <-doneCh:
			log.Println("Received done signal, exiting..")
			return
		default:
			id, seq, success, err := acquireKey(client, key, ttl, sessionName)
			if err != nil {
				l.errorRetryCount++
				goto LABEL
			}
			if success {
				// Maybe the key/session got removed by administrator. Close the renewperiodic channel.
				if l.isLeader {
					close(l.stopSessionRenewCh)
				}
				log.Println("Consul leadership lock acquired. Assuming leadership. Consul session id: ", id)

				stopSessionRenewCh := make(chan struct{})
				l.stopSessionRenewCh = stopSessionRenewCh
				go client.Session().RenewPeriodic(strconv.Itoa(ttl)+"s", id, nil, l.stopSessionRenewCh)
				if !l.isLeader {
					stopCh := make(chan bool)
					l.stopCh = stopCh
					go j.DoJobFunc(l.stopCh, doneCh)
				}
				l.isLeader = true
				l.seq = seq
				time.Sleep(sleepTime * time.Second)
				continue
			}
			// We reached here becoz we could not acquire the key although it is possible that we are still the master.
			log.Println("Consul leadership lock is already acquired.")
			removeSession(client, id)
			if !l.isLeader && onLockFoundExit {
				log.Println("Exiting...")
				os.Exit(0)
			}
		LABEL:
			if l.isLeader {
				if err != nil && l.errorRetryCount > 0 {
					log.Println("I might have lost leadership. Sending stop signal..")
					close(l.stopCh)
					close(l.stopSessionRenewCh)
					l.isLeader = false
					l.seq = nil
				} else if !reflect.DeepEqual(l.seq, seq) {
					log.Println("Lost leadership. Sending stop signal...")
					close(l.stopCh)
					close(l.stopSessionRenewCh)
					l.isLeader = false
					l.seq = nil
				}
			}
			time.Sleep(sleepTime * time.Second)
		}

	}
}
