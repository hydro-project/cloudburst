#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import logging
import pickle
import random
import sys
import time
import uuid

import cloudpickle as cp
import numpy

from anna.lattices import ListBasedOrderedSet
from droplet.shared.proto.droplet_pb2 import DropletError, DAG_ALREADY_EXISTS


# NOTE: If sckt is not none, we will try to read functions instead of registering them;
# the idea is that prepopulation has to be done off of the benchmark nodes
# (and thus the functions themselves must have already been registered).
def run(droplet_client, num_requests, sckt):
    logging.info("Starting retwis_benchmark.run()")
    # ----
    # Hydro redis shim.
    # ----
    class DropletRedisShim:
        def __init__(self, droplet_user_library):
            self._droplet = droplet_user_library

        # Checking for existence of arbitrary keys.
        def exists(self, key):
            value_or_none = self._droplet.get(key)
            return value_or_none is not None

        ## Single value storage.
        # Retrieving arbitrary values that were stored by set().
        def get(self, key):
            value = self._droplet.get(key)
            if value is None: return None
            value = pickle.loads(value)
            return value
        # def get_with_vc(self, key):
        #     if self.exists(key):
        #         vc, values = self._droplet.get(key)
        #         value = values[0]
        #         value = pickle.loads(value)
        #         return vc, value
        #     else:
        #         return None, None
        # Storing arbitary values that can be retrieved by get().
        def set(self, key, value):
            value = pickle.dumps(value)
            self._droplet.put(key, value)
        # Do a set with causal dependencies.
        # Dependencies is {'key': <vc>}
        def causal_set(self, key, value, dependencies):
            value = pickle.dumps(value)

            client_id = str(int(uuid.uuid4()))
            vector_clock = {client_id: 1}
            self._droplet.causal_put(key, vector_clock, dependencies, value, client_id)

        ## Counter storage.
        # incr in Redis is used for two things:
        # - returning unique IDs
        # - as a counter
        # This mocks out the former.
        def incr(self, key):
            return int(uuid.uuid4())

        ## Set storage.
        # Add an item to the set at this key.
        def sadd(self, key, value):
            value = pickle.dumps(value)
            self._droplet.put(key, {value})
        # Remove an item from the set at this key.
        def srem(self, key, value):
            raise NotImplementedError  # No removals in experiments rn; implement tombstones later if you need it.
        # Set contents.
        def smembers(self, key):
            values = self._droplet.get(key)
            if values is None: return set()
            return set((pickle.loads(val) for val in values))
        # Set membership.
        def sismember(self, key):
            return key in self.smembers(key)
        # Set size.
        def scard(self, key):
            return len(self.smembers(key))

        ## Append-only lists.
        # Append.
        def lpush(self, key, value):
            # microseconds.
            # This value will be 16 digits long for the foreseeable future.
            ts = int(time.time() * 1000000)
            value = pickle.dumps(value)
            value = ('{}:{}'.format(ts, value)).encode()
            self._droplet.put(key, [value])
        # Slice.
        def lrange(self, key, begin, end):
            if self.exists(key):
                values = self._droplet.get(key)
                oset = ListBasedOrderedSet(values)
                values = [
                    # trim off timestamp + delimiter, and deserialize the rest.
                    pickle.loads(eval(item.decode()[17:]))
                    for item in oset.lst[begin:end]
                ]
                return values
            else:
                return []
        # Size.
        def llen(self, key):
            if self.exists(key):
                values = self._droplet.get(key)
                return len(list(values))
            else:
                return 0

    ## Retwis library.
    class Timeline:
        @staticmethod
        def page(r,page):
            _from = (page-1)*10
            _to = (page)*10
            return [Post(r, post_id).content for post_id in r.lrange('timeline',_from,_to)]
    class Model(object):
        def __init__(self, r, id):
            self.__dict__['id'] = id
            self.__dict__['r'] = r
        def __eq__(self,other):
            return self.id == other.id
        def __setattr__(self,name,value):
            if name not in self.__dict__:
                klass = self.__class__.__name__.lower()
                key = '%s:id:%s:%s' % (klass,self.id,name.lower())
                self.r.set(key,value)
            else:
                self.__dict__[name] = value
        def __getattr__(self,name):
            if name not in self.__dict__:
                klass = self.__class__.__name__.lower()
                v = self.r.get('%s:id:%s:%s' % (klass,self.id,name.lower()))
                if v:
                    return v
                raise AttributeError('%s doesn\'t exist' % name)
            else:
                return self.__dict__[name]
    class User(Model):
        @staticmethod
        def find_by_user(r, user):
            _id = r.get("user:user:%s" % user)
            if _id is not None:
                return int(_id)
            else:
                return None
        @staticmethod
        def find_by_id(_id):
            if r.exists("user:id:%s:user" % _id):
                return User(int(_id))
            else:
                return None
        @staticmethod
        def create(r, user, password):
            # user_id = r.incr("user:uid")
            user_id = user # for idempotency :)))
            r.get("user:user:%s" % user)
            if not r.get("user:user:%s" % user):
                r.set("user:id:%s:user" % user_id, user)
                r.set("user:user:%s" % user, user_id)
                r.set("user:id:%s:password" % user_id, password)
                r.lpush("users", user_id)
            # return User(user_id)
        def posts(self,page=1):
            _from, _to = (page-1)*10, page*10
            posts = r.lrange("user:id:%s:posts" % self.id, _from, _to)
            if posts:
                return [Post(int(post_id)) for post_id in posts]
            return []
        @staticmethod
        def timeline(r, user, page=1):
            userid = User.find_by_user(r, user)
            timeline_len = r.llen("user:id:%s:timeline" % userid)
            if timeline_len > 0:
                _from, _to = timeline_len - page*10, timeline_len - (page-1)*10,
                timeline = r.lrange("user:id:%s:timeline" % userid, _from, _to)
                if timeline:
                    # XXX waiting on causal consistency is done already,
                    # but we still need to reinsert the dependee tweet.
                    # We can skip this for now since the performance overhead is already incurred;
                    # inserting the actual tweets is just a quick operation for application-semantics correctness.
                    return [(int(post_id), Post(r, int(post_id)).content) for post_id in timeline]
            return []
        @staticmethod
        def timeline_anomalies(r, user):
            userid = User.find_by_user(r, user)
            timeline_len = r.llen("user:id:%s:timeline" % userid)
            _from, _to = 0, timeline_len
            timeline = r.lrange("user:id:%s:timeline" % userid, _from, _to)
            if timeline:
                # XXX waiting on causal consistency is always done upon retrieving post contents,
                # but this says nothing about the state of the timeline, which can have missing postids.
                # Here, we get the list of parent tweets of any replies in the timeline
                # and make sure they're in the timeline too.
                tl_set = set(int(pid) for pid in timeline)
                parents_of_replies = []
                tl_posts = []
                missing_posts = []
                for post_id in timeline:
                    post = Post(r, int(post_id))
                    tl_posts.append((int(post_id), post.content))
                    try:
                        post_parent_id = post.parent
                        logging.info(post_parent_id)
                        parents_of_replies.append(int(post_parent_id))
                    except AttributeError:
                        pass
                # See how many anomalies we got.
                for parent_pid in parents_of_replies:
                    if parent_pid not in tl_set:
                        missing_posts.append((int(parent_pid), Post(r, int(parent_pid)).content))
                return tl_posts, missing_posts
            return [], []
        def mentions(self,page=1):
            _from, _to = (page-1)*10, page*10
            mentions = r.lrange("user:id:%s:mentions" % self.id, _from, _to)
            if mentions:
                return [Post(int(post_id)) for post_id in mentions]
            return []
        @staticmethod
        def add_post(r, userid, post_id):
            r.lpush("user:id:%s:posts" % userid, post_id)
            r.lpush("user:id:%s:timeline" % userid, post_id)
            r.sadd('posts:id', post_id)
        @staticmethod
        def add_timeline_post(r, userid, post_id):
            r.lpush("user:id:%s:timeline" % userid, post_id)
        def add_mention(self,post):
            r.lpush("user:id:%s:mentions" % self.id, post.id)
        @staticmethod
        def follow(r, user, target):
            userid = User.find_by_user(r, user)
            targetid = User.find_by_user(r, target)
            if userid == targetid:
                return
            else:
                r.sadd("user:id:%s:followees" % userid, targetid)
                User.add_follower(r, targetid, userid)
        def stop_following(self,user):
            r.srem("user:id:%s:followees" % self.id, user.id)
            user.remove_follower(self)
        def following(self,user):
            if r.sismember("user:id:%s:followees" % self.id, user.id):
                return True
            return False
        @staticmethod
        def followers(r, userid):
            followers = r.smembers("user:id:%s:followers" % userid)
            if followers:
                return followers
            return []
        @staticmethod
        def followees(userid):
            followees = r.smembers("user:id:%s:followees" % userid)
            if followees:
                return followers
            return []
        #added
        @property
        def tweet_count(self):
            return r.llen("user:id:%s:posts" % self.id) or 0
        @property
        def followees_count(self):
            return r.scard("user:id:%s:followees" % self.id) or 0
        @property
        def followers_count(self):
            return r.scard("user:id:%s:followers" % self.id) or 0
        @staticmethod
        def add_follower(r, userid, targetid):
            r.sadd("user:id:%s:followers" % userid, targetid)
        def remove_follower(self,user):
            r.srem("user:id:%s:followers" % self.id, user.id)
    class Post(Model):
        @staticmethod
        def create(r, user, content, parent_post_id=None):
            userid = User.find_by_user(r, user)
            post_id = r.incr("post:uid")
            post = Post(r, post_id)
            post.user_id = userid
            # #post.created_at = Time.now.to_s

            # Handle replies. If we're replying to a tweet,
            # parent_content_key is the key of that tweet's content.

            # Set this tweet's parent (for application use, not kvs consistency).
            post.parent = parent_post_id
            post.content = content

            User.add_post(r, userid, post_id)
            # r.lpush("timeline", post_id)    # not testing global timeline
            for follower in User.followers(r, userid):
                User.add_timeline_post(r, follower, post_id)
            # mentions = re.findall('@\w+', content)
            # for mention in mentions:
            #     u = User.find_by_user(mention[1:])
            #     if u:
            #         u.add_mention(post)
        @staticmethod
        def create_causal(r, user, content, parent_post_id=None):
            userid = User.find_by_user(r, user)
            post_id = r.incr("post:uid")
            post = Post(r, post_id)
            post.user_id = userid
            # #post.created_at = Time.now.to_s

            # Handle replies. If we're replying to a tweet,
            # parent_content_key is the key of that tweet's content.

            # Set this tweet's parent (for application use, not kvs consistency).
            post.parent = parent_post_id

            if parent_post_id is None:
                post.content = content
            else:
                klass = post.__class__.__name__.lower()
                # Convert the parent post id to its post content id.
                parent_content_key = '%s:id:%s:%s' % (klass, parent_post_id, 'content')
                # Get the parent tweet content's VC; we need it to express a dependency on it.
                vc, _ = r.get_with_vc(parent_content_key)
                # Post our tweet, with the causal dependency.
                r.causal_set('%s:id:%s:%s' % (klass,post.id,'content'), content, {parent_content_key: vc})

            User.add_post(r, userid, post_id)
            # r.lpush("timeline", post_id)    # not testing global timeline
            for follower in User.followers(r, userid):
                User.add_timeline_post(r, follower, post_id)
            # mentions = re.findall('@\w+', content)
            # for mention in mentions:
            #     u = User.find_by_user(mention[1:])
            #     if u:
            #         u.add_mention(post)

        @staticmethod
        def find_by_id(id):
            if r.sismember('posts:id', int(id)):
                return Post(id)
            return None
        @property
        def user(self):
            return User.find_by_id(r.get("post:id:%s:user_id" % self.id))


    ## Defining functions.
    def ccc_redis_exists(droplet, key):
        redis = DropletRedisShim(droplet)
        return str(redis.exists(key))
    def ccc_redis_get(droplet, key):
        redis = DropletRedisShim(droplet)
        return str(redis.get(key))
    def ccc_redis_set(droplet, key, value):
        redis = DropletRedisShim(droplet)
        redis.set(key, value)
        return 'success'
    def ccc_redis_incr(droplet, key):
        redis = DropletRedisShim(droplet)
        return str(redis.incr(key))
    def ccc_redis_sadd(droplet, key, value):
        redis = DropletRedisShim(droplet)
        redis.sadd(key, value)
        return 'success'
    def ccc_redis_smembers(droplet, key):
        redis = DropletRedisShim(droplet)
        return str(redis.smembers(key))
    def ccc_redis_lpush(droplet, key, value):
        redis = DropletRedisShim(droplet)
        redis.lpush(key, value)
        return 'success'
    def ccc_redis_lrange(droplet, key, begin, end):
        redis = DropletRedisShim(droplet)
        return str(redis.lrange(key, begin, end))
    def ccc_redis_llen(droplet, key):
        redis = DropletRedisShim(droplet)
        return str(redis.llen(key))
    # def ccc_global_timeline(droplet, page):
    #     redis = DropletRedisShim(droplet)
    #     return Timeline.page(redis, page)

    def ccc_user_create(droplet, user):
        redis = DropletRedisShim(droplet)
        # droplet.put('ccc_user', SetLattice({b'hi',}))
        # droplet.put('ccc_userb', OrderedSetLattice(ListBasedOrderedSet([b'hi'])))
        User.create(redis, user, 'password')
        return 'success'
    def ccc_user_timeline(droplet, user, page):
        redis = DropletRedisShim(droplet)
        return pickle.dumps(User.timeline(redis, user, page))
    def ccc_user_timeline_anomalies(droplet, user):
        redis = DropletRedisShim(droplet)
        return pickle.dumps(User.timeline_anomalies(redis, user))
    def ccc_user_profile(droplet, user, page):
        pass
    def ccc_user_follow(droplet, user, target):
        redis = DropletRedisShim(droplet)
        User.follow(redis, user, target)
        return 'success'
    def ccc_post_create(droplet, user, post):
        redis = DropletRedisShim(droplet)
        Post.create(redis, user, post)
        return 'success'
    def ccc_reply_create(droplet, user, post, parent_cid):
        redis = DropletRedisShim(droplet)
        Post.create(redis, user, post, parent_cid)
        return 'success'
    def ccc_reply_create_causal(droplet, user, post, parent_cid):
        redis = DropletRedisShim(droplet)
        Post.create_causal(redis, user, post, parent_cid)
        return 'success'
    # def ccc_test_empty(droplet):
    #     return []
    fns = {
        'ccc_redis_exists': ccc_redis_exists,
        'ccc_redis_get': ccc_redis_get,
        'ccc_redis_set': ccc_redis_set,
        'ccc_redis_incr': ccc_redis_incr,
        'ccc_redis_sadd': ccc_redis_sadd,
        'ccc_redis_smembers': ccc_redis_smembers,
        'ccc_redis_lpush': ccc_redis_lpush,
        'ccc_redis_lrange': ccc_redis_lrange,
        'ccc_redis_llen': ccc_redis_llen,
        # 'ccc_global_timeline': ccc_global_timeline,
        'ccc_user_create': ccc_user_create,
        'ccc_user_timeline': ccc_user_timeline,
        'ccc_user_timeline_anomalies': ccc_user_timeline_anomalies,
        'ccc_user_profile': ccc_user_profile,
        'ccc_user_follow': ccc_user_follow,
        'ccc_post_create': ccc_post_create,
        'ccc_reply_create': ccc_reply_create,
        'ccc_reply_create_causal': ccc_reply_create_causal,
        # 'ccc_test_empty': ccc_test_empty,
    }


    ## Register functions.
    logging.info("Registering functions.")
    if sckt is None:
        cfns = {
            fname: droplet_client.register(f, fname)
            for fname, f
            in fns.items()
        }
        for fname, cf in cfns.items():
            if cf:
                logging.info("Successfully registered {}.".format(fname))
            else:
                sys.exit(1)
    else:
        cfns = {
            fname: droplet_client.get_function(fname)
            for fname, f
            in fns.items()
        }
        for fname, cf in cfns.items():
            if cf:
                logging.info("Successfully retrieved {}.".format(fname))
            else:
                sys.exit(1)
    def callfn(fname, *args):
        r = cfns[fname](*args).get()
        logging.info("%s(%s) -> %s" % (fname, args, r))
        return r

    # Redis shim tests (not retwis related).
    logging.info("Testing redis shim functionality.")
    callfn('ccc_redis_exists', 'ccc_foo')
    callfn('ccc_redis_set', 'ccc_foo', b'3')
    callfn('ccc_redis_get', 'ccc_foo')
    callfn('ccc_redis_incr', 'ccc_cntr')
    callfn('ccc_redis_sadd', 'ccc_sxt2', b'4')
    callfn('ccc_redis_smembers', 'ccc_sxt2')
    callfn('ccc_redis_lpush', 'ccc_lxt', b'5')
    callfn('ccc_redis_lrange', 'ccc_lxt', 0, 10)
    callfn('ccc_redis_llen', 'ccc_lxt')
    callfn('ccc_redis_lpush', 'ccc_lxt', b'6')
    callfn('ccc_redis_lpush', 'ccc_lxt', b'4')
    callfn('ccc_redis_lrange', 'ccc_lxt', 0, 10)
    # callfn('ccc_test_empty')

    # Experiment parameters.
    # ######################
    if num_requests < 1000:
        num_users = 100
        max_degree = 10
        num_pretweets = 100
        num_ops = num_requests  # 80% reads, 20% writes
        usernames = [str(i + 1) for i in range(num_users)]
        reply_frac = 0.2
        count_anomalies = False
        logging_rate = 1
    else:
        num_users = 1000
        max_degree = 30
        num_pretweets = 5000
        num_ops = num_requests  # 80% reads, 20% writes
        usernames = [str(i + 1) for i in range(num_users)]
        reply_frac = 0.2
        count_anomalies = False
        logging_rate = 1

    # for user in usernames:
    # callfn('ccc_user_timeline', '1', 1)

    # Experiment helpers.
    def get_random_user():  # -> str
        return str(int(random.random() * num_users) + 1)
    def get_zipf_user():
        a = 1.5  # "realistic social network distribution" from johann
        res = numpy.random.zipf(1.5)
        while res > num_users:
            res = numpy.random.zipf(1.5)
        return str(res)
    def get_n_zipf_users(n):
        users = set()
        while len(users) < n:
            users.add(get_zipf_user())
        return users
    # Posts a reply to the most recent tweet in the user's timeline, if available.
    # Does nothing if there are no tweets in the timeline to reply to!
    def post_random_reply(username):
        res = cfns['ccc_user_timeline'](username, 1).get()
        res = pickle.loads(res)
        if len(res) == 0: return False
        parent_tweet_cid = res[-1][0]
        # logging.info(parent_tweet_cid)
        # logging.info(username)
        # logging.info("{} says: @{}, agreed!".format(username, parent_tweet_cid))
        res = cfns['ccc_reply_create'](
            username, "{} says: @{}, agreed!".format(username, parent_tweet_cid), parent_tweet_cid).get()
        # res = cfns['ccc_reply_create'](
        #     username, "{} says: @{}, agreed!".format(username, parent_tweet_cid), parent_tweet_cid).get()
        return True


    # Again, only do prepopulation if we're running from the devserver.
    if sckt is None:

        # Make all the users.
        log_start = time.time()
        logging.info("Making %s users..." % num_users)
        for username in usernames:
            res = cfns['ccc_user_create'](username).get()
            if res != 'success':
                logging.info("ccc_user_create(%s) -> %s" % (username, str(res)))
                sys.exit(1)
            if time.time() - log_start > logging_rate:
                logging.info("Currently making user %s." % username)
                log_start = time.time()

        # Do follows.
        log_start = time.time()
        logging.info("Doing %s follows per user..." % max_degree)
        for username in usernames:
            targets = get_n_zipf_users(max_degree)
            for i, target in enumerate(targets):
                res = cfns['ccc_user_follow'](username, target).get()
                if res != 'success':
                    logging.info("ccc_user_follow(%s, %s) -> %s" % (username, target, str(res)))
                    sys.exit(1)
                if time.time() - log_start > logging_rate:
                    logging.info("Currently at %s following %sth target." % (username, i))
                    log_start = time.time()

        # Prepopulate tweets.
        log_start = time.time()
        logging.info("Prepopulating %s tweets total..." % num_pretweets)
        for i in range(num_pretweets):
            username = get_random_user()
            post = "{} says: I love droplet!".format(username)
            # Let's make some of these tweets be replies to other tweets.
            t = random.random()
            start = time.time()
            tweeted = False
            if t < reply_frac: # Reply attempt factor.
                tweeted = post_random_reply(username)
            if not tweeted:
                cfns['ccc_post_create'](username, post).get()
            if time.time() - log_start > logging_rate:
                logging.info("%s tweets populated." % i)
                log_start = time.time()
            # cfns['ccc_post_create'](username, post).get()
            # elapsed = time.time() - start
            # wtimes.append(elapsed)
            # epoch_wtimes.append(elapsed)
            # if time.time() - log_start > 5:
            #     logging.info("%s tweets populated." % i)
            #     # if sckt:
            #     #     sckt.send(cp.dumps(epoch_wtimes))
            #     epoch_wtimes.clear()



    # Execute workload.
    log_start = time.time()
    logging.info("Executing %s ops workload..." % num_ops)
    rtimes = []
    wtimes = []
    start = time.time()
    tl_lengths = []
    anomaly_counts = []
    for numop in range(num_ops):
        if time.time() - log_start > logging_rate:
            logging.info("%s num ops done." % numop)
            log_start = time.time()
        t = random.random()
        # Pick a user at uniform.
        username = get_random_user()
        # 80% reads.
        if t < 0.8:
            r_start = time.time()
            if count_anomalies:
                res, anomalies = pickle.loads(cfns['ccc_user_timeline_anomalies'](username).get())
                anomaly_counts.append(len(anomalies))
                tl_lengths.append(len(res))
            else:
                res = pickle.loads(cfns['ccc_user_timeline'](username, 1).get())
            rtimes.append(time.time() - r_start)

        # 20% writes.
        else:
            w_start = time.time()
            tweeted = False
            reply_roll = random.random()
            if reply_roll < reply_frac:
                tweeted = post_random_reply(username)
            if not tweeted:
                post = "{} says: I LOVE droplet!".format(username)
                res = cfns['ccc_post_create'](username, post).get()
            wtimes.append(time.time() - w_start)

    end = time.time()
    elapsed = end - start


    # Sanity check: print timeline of most and least popular user.
    res = pickle.loads(cfns['ccc_user_timeline']('1', 1).get())
    logging.info("ccc_user_timeline('1', 1) -> %s" % (str(res)))
    res = pickle.loads(cfns['ccc_user_timeline'](str(num_users), 1).get())
    logging.info("ccc_user_timeline(%s, 1) -> %s" % (str(num_users), str(res)))

    total_time = [0]
    scheduler_time = [0]
    kvs_time = [0]
    total_time = rtimes
    scheduler_time = wtimes

    retries = 0

    if sckt:
        sckt.send(cp.dumps((rtimes, wtimes)))
    return total_time, scheduler_time, kvs_time, retries
