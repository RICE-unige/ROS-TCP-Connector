using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;
using UnityEngine.Serialization;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;

namespace Unity.Robotics.ROSTCPConnector
{
    public class ROSConnection : MonoBehaviour
    {
        public const string k_Version = "v0.7.0";
        public const string k_CompatibleVersionPrefix = "v0.7.";

        // Variables required for ROS communication
        [SerializeField]
        [FormerlySerializedAs("hostName")]
        [FormerlySerializedAs("rosIPAddress")]
        string m_RosIPAddress = "127.0.0.1";
        public string RosIPAddress { get => m_RosIPAddress; set => m_RosIPAddress = value; }

        [SerializeField]
        [FormerlySerializedAs("hostPort")]
        [FormerlySerializedAs("rosPort")]
        int m_RosPort = 10000;
        public int RosPort { get => m_RosPort; set => m_RosPort = value; }

        [SerializeField]
        bool m_ConnectOnStart = true;
        public bool ConnectOnStart { get => m_ConnectOnStart; set => m_ConnectOnStart = value; }

        [SerializeField]
        [Tooltip("If nothing has been sent for this long (seconds), send a keepalive message to check the connection is still working.")]
        float m_KeepaliveTime = 1;
        public float KeepaliveTime { get => m_KeepaliveTime; set => m_KeepaliveTime = value; }

        [SerializeField]
        float m_NetworkTimeoutSeconds = 2;
        public float NetworkTimeoutSeconds { get => m_NetworkTimeoutSeconds; set => m_NetworkTimeoutSeconds = value; }

        [SerializeField]
        float m_SleepTimeSeconds = 0.01f;
        public float SleepTimeSeconds { get => m_SleepTimeSeconds; set => m_SleepTimeSeconds = value; }


        [SerializeField]
        [FormerlySerializedAs("showHUD")]
        bool m_ShowHUD = true;
        public bool ShowHud { get => m_ShowHUD; set => m_ShowHUD = value; }

        [SerializeField]
        string[] m_TFTopics = { "/tf" };
        public string[] TFTopics { get => m_TFTopics; set => m_TFTopics = value; }

        const int k_DefaultPublisherQueueSize = 10;
        const bool k_DefaultPublisherLatch = false;

        // GUI window variables
        internal HudPanel m_HudPanel = null;
        public HudPanel HUDPanel => m_HudPanel;

        class OutgoingMessageQueue
        {
            ConcurrentQueue<OutgoingMessageSender> m_OutgoingMessageQueue;
            public readonly ManualResetEvent NewMessageReadyToSendEvent;

            public OutgoingMessageQueue()
            {
                m_OutgoingMessageQueue = new ConcurrentQueue<OutgoingMessageSender>();
                NewMessageReadyToSendEvent = new ManualResetEvent(false);
            }

            public void Enqueue(OutgoingMessageSender outgoingMessageSender)
            {
                m_OutgoingMessageQueue.Enqueue(outgoingMessageSender);
                NewMessageReadyToSendEvent.Set();
            }

            public bool TryDequeue(out OutgoingMessageSender outgoingMessageSender)
            {
                return m_OutgoingMessageQueue.TryDequeue(out outgoingMessageSender);
            }
        }

        OutgoingMessageQueue m_OutgoingMessageQueue = new OutgoingMessageQueue();

        ConcurrentQueue<Tuple<string, byte[]>> m_IncomingMessages = new ConcurrentQueue<Tuple<string, byte[]>>();
        CancellationTokenSource m_ConnectionThreadCancellation;
        public bool HasConnectionThread => m_ConnectionThreadCancellation != null;

        static bool m_HasConnectionError = false;
        public bool HasConnectionError => m_HasConnectionError;

        // only the main thread can access Time.*, so make a copy here
        public static float s_RealTimeSinceStartup = 0.0f;

        readonly object m_ServiceRequestLock = new object();

        int m_NextSrvID = 101;
        Dictionary<int, TaskPauser> m_ServicesWaiting = new Dictionary<int, TaskPauser>();

        public bool listenForTFMessages = true;

        float m_LastMessageReceivedRealtime;
        float m_LastMessageSentRealtime;
        public float LastMessageReceivedRealtime => m_LastMessageReceivedRealtime;
        public float LastMessageSentRealtime => m_LastMessageSentRealtime;

        // For the IP address field we show in the hud, we store the IP address and port in PlayerPrefs.
        // This is used to remember the last IP address the player typed into the HUD, in builds where ConnectOnStart is not checked
        public const string PlayerPrefsKey_ROS_IP = "ROS_IP";
        public const string PlayerPrefsKey_ROS_TCP_PORT = "ROS_TCP_PORT";
        public static string RosIPAddressPref => PlayerPrefs.GetString(PlayerPrefsKey_ROS_IP, "127.0.0.1");
        public static int RosPortPref => PlayerPrefs.GetInt(PlayerPrefsKey_ROS_TCP_PORT, 10000);

        public bool HasSubscriber(string topic)
        {
            RosTopicState info;
            return m_Topics.TryGetValue(topic, out info) && info.HasSubscriberCallback;
        }

        MessageSerializer m_MessageSerializer = new MessageSerializer();
        MessageDeserializer m_MessageDeserializer = new MessageDeserializer();
        List<Action<string[]>> m_TopicsListCallbacks = new List<Action<string[]>>();
        List<Action<Dictionary<string, string>>> m_TopicsAndTypesListCallbacks = new List<Action<Dictionary<string, string>>>();
        List<Action<RosTopicState>> m_NewTopicCallbacks = new List<Action<RosTopicState>>();

        Dictionary<string, RosTopicState> m_Topics = new Dictionary<string, RosTopicState>();

        // OPTIONAL declarative outbound lane/QoS overrides, keyed by topic. Populated by
        // SetSubscribePolicy and consulted when a subscriber registration is sent. Empty by
        // default, so topics without an entry use the existing (heuristic) bridge classification.
        Dictionary<string, string> m_SubscribePolicies = new Dictionary<string, string>();

        // Declare an explicit outbound transport lane/QoS policy for a topic's bridge subscription.
        // Valid values: "strict", "replaceable", "bulk_strict", "bulk_replaceable". Pass null or
        // empty to clear a previously declared policy (reverting to the bridge's heuristic lane).
        // Must be called before the topic is subscribed for the policy to be included in the
        // initial __subscribe; calling it later affects subsequent (re)registrations.
        public void SetSubscribePolicy(string topic, string policy)
        {
            if (string.IsNullOrEmpty(topic))
            {
                return;
            }

            lock (m_SubscribePolicies)
            {
                if (string.IsNullOrWhiteSpace(policy))
                {
                    m_SubscribePolicies.Remove(topic);
                }
                else
                {
                    m_SubscribePolicies[topic] = policy.Trim().ToLowerInvariant();
                }
            }
        }

        string GetSubscribePolicy(string topic)
        {
            if (string.IsNullOrEmpty(topic))
            {
                return null;
            }

            lock (m_SubscribePolicies)
            {
                return m_SubscribePolicies.TryGetValue(topic, out string policy) ? policy : null;
            }
        }

        public void ListenForTopics(Action<RosTopicState> callback, bool notifyAllExistingTopics = false)
        {
            m_NewTopicCallbacks.Add(callback);
            if (notifyAllExistingTopics)
            {
                foreach (RosTopicState state in AllTopics)
                {
                    callback(state);
                }
            }
        }

        RosTopicState AddTopic(string topic, string rosMessageName, bool isService = false)
        {
            RosTopicState newTopic = new RosTopicState(topic, rosMessageName, this, new InternalAPI(this), isService);
            lock (m_Topics)
            {
                m_Topics.Add(topic, newTopic);
            }
            return newTopic;
        }

        public RosTopicState GetTopic(string topic)
        {
            RosTopicState info;
            m_Topics.TryGetValue(topic, out info);
            return info;
        }

        public IEnumerable<RosTopicState> AllTopics => m_Topics.Values;

        public RosTopicState GetOrCreateTopic(string topic, string rosMessageName, bool isService = false)
        {
            RosTopicState state = GetTopic(topic);
            if (state != null)
            {
                if (state.RosMessageName != rosMessageName)
                {
                    state.ChangeRosMessageName(rosMessageName);
                }
                return state;
            }

            RosTopicState result = AddTopic(topic, rosMessageName, isService);
            foreach (Action<RosTopicState> callback in m_NewTopicCallbacks)
            {
                callback(result);
            }
            return result;
        }

        public void Subscribe<T>(string topic, Action<T> callback) where T : Message
        {
            string rosMessageName = MessageRegistry.GetRosMessageName<T>();
            AddSubscriberInternal(topic, rosMessageName, (Message msg) =>
            {
                if (msg.RosMessageName == rosMessageName)
                {
                    callback((T)msg);
                }
                else
                {
                    Debug.LogError($"Subscriber to '{topic}' expected '{rosMessageName}' but received '{msg.RosMessageName}'!?");
                }
            });
        }

        public void SubscribeRaw(string topic, string rosMessageName, Action<byte[]> callback, string policy = null)
        {
            if (string.IsNullOrEmpty(topic) || string.IsNullOrEmpty(rosMessageName) || callback == null)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(policy))
            {
                SetSubscribePolicy(topic, policy);
            }

            AddRawSubscriberInternal(topic, rosMessageName, callback);
        }

        public void Unsubscribe(string topic)
        {
            RosTopicState info = GetTopic(topic);
            if (info != null)
                info.UnsubscribeAll();
        }

        // Version for when the message type is unknown at compile time
        public void SubscribeByMessageName(string topic, string rosMessageName, Action<Message> callback)
        {
            var constructor = MessageRegistry.GetDeserializeFunction(rosMessageName);
            if (constructor == null)
            {
                Debug.LogError($"Failed to subscribe to topic {topic} - no class has RosMessageName \"{rosMessageName}\"!");
                return;
            }

            AddSubscriberInternal(topic, rosMessageName, callback);
        }

        void AddSubscriberInternal(string topic, string rosMessageName, Action<Message> callback)
        {
            RosTopicState info;
            if (!m_Topics.TryGetValue(topic, out info))
            {
                info = AddTopic(topic, rosMessageName);
            }

            info.AddSubscriber(callback);

            foreach (Action<RosTopicState> topicCallback in m_NewTopicCallbacks)
            {
                topicCallback(info);
            }
        }

        void AddRawSubscriberInternal(string topic, string rosMessageName, Action<byte[]> callback)
        {
            if (callback == null)
            {
                return;
            }

            RosTopicState info;
            if (!m_Topics.TryGetValue(topic, out info))
            {
                info = AddTopic(topic, rosMessageName);
            }

            info.AddRawSubscriber(callback);

            foreach (Action<RosTopicState> topicCallback in m_NewTopicCallbacks)
            {
                topicCallback(info);
            }
        }

        // Implement a service in Unity
        public void ImplementService<TRequest, TResponse>(string topic, Func<TRequest, TResponse> callback, int? queueSize = null)
            where TRequest : Message
            where TResponse : Message
        {
            string rosMessageName = rosMessageName = MessageRegistry.GetRosMessageName<TRequest>();

            RosTopicState info;
            if (!m_Topics.TryGetValue(topic, out info))
            {
                info = AddTopic(topic, rosMessageName, isService: true);
            }

            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            info.ImplementService(callback, resolvedQueueSize);

            foreach (Action<RosTopicState> topicCallback in m_NewTopicCallbacks)
            {
                topicCallback(info);
            }
        }

        // Implement a service in Unity
        public void ImplementService<TRequest, TResponse>(string topic, Func<TRequest, Task<TResponse>> callback, int? queueSize = null)
            where TRequest : Message
            where TResponse : Message
        {
            string rosMessageName = rosMessageName = MessageRegistry.GetRosMessageName<TRequest>();

            RosTopicState info;
            if (!m_Topics.TryGetValue(topic, out info))
            {
                info = AddTopic(topic, rosMessageName, isService: true);
            }

            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            info.ImplementService(callback, resolvedQueueSize);

            foreach (Action<RosTopicState> topicCallback in m_NewTopicCallbacks)
            {
                topicCallback(info);
            }
        }

        // Send a request to a ros service
        public async void SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest, Action<RESPONSE> callback) where RESPONSE : Message, new()
        {
            RESPONSE response = await SendServiceMessage<RESPONSE>(rosServiceName, serviceRequest);
            try
            {
                callback(response);
            }
            catch (Exception e)
            {
                Debug.LogError("Exception in service callback: " + e);
            }
        }

        // Send a request to a ros service
        public async Task<RESPONSE> SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest) where RESPONSE : Message, new()
        {
            m_MessageSerializer.Clear();
            m_MessageSerializer.SerializeMessage(serviceRequest);
            byte[] requestBytes = m_MessageSerializer.GetBytes();
            TaskPauser pauser = new TaskPauser();

            int srvID;
            lock (m_ServiceRequestLock)
            {
                srvID = m_NextSrvID++;
                m_ServicesWaiting.Add(srvID, pauser);
            }

            RosTopicState topicState = GetOrCreateTopic(rosServiceName, serviceRequest.RosMessageName, isService: true);
            topicState.SendServiceRequest(serviceRequest, srvID);

            byte[] rawResponse = (byte[])await pauser.PauseUntilResumed();

            topicState.OnMessageReceived(rawResponse);
            RESPONSE result = m_MessageDeserializer.DeserializeMessage<RESPONSE>(rawResponse);
            return result;
        }

        public void GetTopicList(Action<string[]> callback)
        {
            m_TopicsListCallbacks.Add(callback);
            SendSysCommand(SysCommand.k_SysCommand_TopicList, new SysCommand_TopicsRequest());
        }

        public void GetTopicAndTypeList(Action<Dictionary<string, string>> callback)
        {
            m_TopicsAndTypesListCallbacks.Add(callback);
            SendSysCommand(SysCommand.k_SysCommand_TopicList, new SysCommand_TopicsRequest());
        }

        [Obsolete("Calling Subscribe now implicitly registers a subscriber")]
        public void RegisterSubscriber(string topic, string rosMessageName)
        {
        }

        public RosTopicState RegisterPublisher<T>(string rosTopicName,
            int? queue_size = null, bool? latch = null) where T : Message
        {
            return RegisterPublisher(rosTopicName, MessageRegistry.GetRosMessageName<T>(), queue_size, latch);
        }

        public RosTopicState RegisterPublisher(string rosTopicName, string rosMessageName,
            int? queueSize = null, bool? latch = null)
        {
            RosTopicState topicState = GetOrCreateTopic(rosTopicName, rosMessageName);
            //Create a new publisher.
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            bool resolvedLatch = latch.GetValueOrDefault(k_DefaultPublisherLatch);
            topicState.RegisterPublisher(resolvedQueueSize, resolvedLatch);
            return topicState;
        }

        public void RegisterRosService<TRequest, TResponse>(string topic) where TRequest : Message where TResponse : Message
        {
            RegisterRosService(topic, MessageRegistry.GetRosMessageName<TRequest>(), MessageRegistry.GetRosMessageName<TResponse>());
        }

        public void RegisterRosService(string topic, string requestMessageName, string responseMessageName, int? queueSize = null)
        {
            RosTopicState info = GetOrCreateTopic(topic, requestMessageName, isService: true);
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            info.RegisterRosService(responseMessageName, resolvedQueueSize);
        }

        [Obsolete("Calling ImplementUnityService now implicitly registers it")]
        public void RegisterUnityService(string topic, string rosMessageName)
        {
        }

        internal struct InternalAPI
        {
            ROSConnection m_Self;

            public InternalAPI(ROSConnection self) { m_Self = self; }

            public MessageDeserializer Deserializer => m_Self.m_MessageDeserializer;

            public void SendSubscriberRegistration(string topic, string rosMessageName, NetworkStream stream = null)
            {
                // If a declarative transport lane was registered for this topic, forward it as the
                // "policy" key so the bridge honors it verbatim; otherwise send the unchanged plain
                // registration and let the bridge classify the lane heuristically (default behavior).
                string policy = m_Self.GetSubscribePolicy(topic);
                if (!string.IsNullOrEmpty(policy))
                {
                    m_Self.SendSysCommand(
                        SysCommand.k_SysCommand_Subscribe,
                        new SysCommand_SubscribeWithPolicy { topic = topic, message_name = rosMessageName, policy = policy },
                        stream);
                }
                else
                {
                    m_Self.SendSysCommand(SysCommand.k_SysCommand_Subscribe, new SysCommand_TopicAndType { topic = topic, message_name = rosMessageName }, stream);
                }
            }

            public void SendRosServiceRegistration(string topic, string rosMessageName, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RosService, new SysCommand_TopicAndType { topic = topic, message_name = rosMessageName }, stream);
            }

            public void SendUnityServiceRegistration(string topic, string rosMessageName, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_UnityService, new SysCommand_TopicAndType { topic = topic, message_name = rosMessageName }, stream);
            }

            public void SendSubscriberUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemoveSubscriber, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendPublisherUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemovePublisher, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendRosServiceUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemoveRosService, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendUnityServiceUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemoveUnityService, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendUnityServiceResponse(int serviceId, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_ServiceResponse, new SysCommand_Service { srv_id = serviceId }, stream);
            }

            public void SendPublisherRegistration(string topic, string message_name, int queueSize, bool latch, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_Publish,
                    new SysCommand_PublisherRegistration { topic = topic, message_name = message_name, queue_size = queueSize, latch = latch }
                );
            }

            public void SendServiceRequest(int serviceId)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_ServiceRequest, new SysCommand_Service { srv_id = serviceId });
            }

            public void AddSenderToQueue(OutgoingMessageSender sender)
            {
                m_Self.m_OutgoingMessageQueue.Enqueue(sender);
            }
        }

        static ROSConnection _instance;

        public static ROSConnection GetOrCreateInstance()
        {
            if (_instance == null)
            {
                // Prefer to use the ROSConnection in the scene, if any
                _instance = FindObjectOfType<ROSConnection>();
                if (_instance != null)
                    return _instance;

                GameObject prefab = Resources.Load<GameObject>("ROSConnectionPrefab");
                if (prefab == null)
                {
                    Debug.LogWarning("No settings for ROSConnection.instance! Open \"ROS Settings\" from the Robotics menu to configure it.");
                    GameObject instance = new GameObject("ROSConnection");
                    _instance = instance.AddComponent<ROSConnection>();
                }
                else
                {
                    _instance = Instantiate(prefab).GetComponent<ROSConnection>();
                }
            }
            return _instance;
        }

        [Obsolete("Please call ROSConnection.GetOrCreateInstance()")]
        public static ROSConnection instance
        {
            get
            {
                return GetOrCreateInstance();
            }
        }

        void Awake()
        {
            if (_instance == null)
                _instance = this;
        }

        void Start()
        {
            InitializeHUD();

            HudPanel.RegisterHeader(DrawHeaderGUI);

            if (listenForTFMessages)
                TFSystem.GetOrCreateInstance();

            if (ConnectOnStart)
                Connect();
        }

        public void Connect(string ipAddress, int port)
        {
            RosIPAddress = ipAddress;
            RosPort = port;
            Connect();
        }

        public void Connect()
        {
            if (!IPFormatIsCorrect(RosIPAddress))
                Debug.LogWarning("Invalid ROS IP address: " + RosIPAddress);

            m_ConnectionThreadCancellation = new CancellationTokenSource();

            Task.Run(() => ConnectionThread(
                RosIPAddress,
                RosPort,
                m_NetworkTimeoutSeconds,
                m_KeepaliveTime,
                (int)(m_SleepTimeSeconds * 1000.0f),
                OnConnectionStartedCallback,
                OnConnectionLostCallback,
                m_OutgoingMessageQueue,
                m_IncomingMessages,
                m_ConnectionThreadCancellation.Token
            ));
        }

        // NB this callback is not running on the main thread, be cautious about modifying data here
        void OnConnectionStartedCallback(NetworkStream stream)
        {
            RosTopicState[] topics;
            lock (m_Topics)
            {
                topics = AllTopics.ToArray();
            }

            foreach (RosTopicState topicInfo in m_Topics.Values.ToArray())
                topicInfo.OnConnectionEstablished(stream);

            RefreshTopicsList();
        }

        void OnConnectionLostCallback()
        {
            RosTopicState[] topics;
            lock (m_Topics)
            {
                topics = AllTopics.ToArray();
            }

            foreach (RosTopicState topicInfo in topics)
            {
                //For all publishers, notify that they need to re-register.
                topicInfo.OnConnectionLost();
            }
        }

        public void Disconnect()
        {
            m_ConnectionThreadCancellation?.Cancel();
            //The thread may be waiting on a ManualResetEvent, if so, this will wake it so it can exit immediately.
            m_OutgoingMessageQueue?.NewMessageReadyToSendEvent?.Set();
            m_ConnectionThreadCancellation = null;
        }

        void OnValidate()
        {
            // the prefab is not the instance!
            if (gameObject.scene.name == null)
                return;

            if (_instance == null)
                _instance = this;
        }

        Action<string, byte[]> m_SpecialIncomingMessageHandler;

        // ---------------------------------------------------------------------
        // HORUS additive optimization: Realtime-first, time-budgeted ingestion.
        //
        // The stock pump (below, the original code path) drains the entire
        // incoming queue and deserializes every message synchronously inside a
        // single Update(). A burst of large "bulk/map" messages (PointCloud2,
        // mesh markers, gaussian-splat chunks, occupancy grids, registry
        // replays, ...) can therefore stall the main thread and delay
        // latency-sensitive "realtime" traffic (camera, TF, laser scan, odom).
        //
        // When EnableRealtimeFirstPump is true, Update() instead:
        //   (a) drains the queue ONCE into a local buffer (FIFO order preserved),
        //   (b) processes realtime / syscommand / service-handshake messages
        //       immediately, in order,
        //   (c) processes remaining bulk messages under a bounded per-frame time
        //       budget, carrying any remainder to the next Update() with their
        //       relative order intact.
        //
        // This is LOSSLESS: no message is dropped, no payload is altered, and
        // per-topic message order is preserved (a topic is classified as either
        // realtime or bulk, never split across both lanes).
        //
        // SAFETY / DEFAULT-OFF: the flag defaults to false, so the shipped
        // behavior is byte-for-byte the original loop. Turn it on from Unity via
        // ROSConnection.EnableRealtimeFirstPump = true after populating
        // RealtimeTopics / RealtimeTopicPredicate. Service request/response
        // routing relies on m_SpecialIncomingMessageHandler consuming the *next*
        // message in strict order; to never corrupt that, the budgeted path
        // falls back to strict in-order draining whenever a special handler is
        // pending or a syscommand that arms one is encountered.
        // ---------------------------------------------------------------------

        /// <summary>
        /// Opt-in: when true, Update() prioritizes realtime topics and time-slices
        /// bulk/map deserialization. Defaults to false (original behavior).
        /// </summary>
        public static bool EnableRealtimeFirstPump = false;

        /// <summary>
        /// Per-frame wall-clock budget (milliseconds) for deserializing bulk
        /// (non-realtime) messages once the realtime lane has been drained.
        /// Realtime and syscommand traffic is always fully drained and is not
        /// charged against this budget. Only consulted when
        /// <see cref="EnableRealtimeFirstPump"/> is true.
        /// </summary>
        public static float BulkProcessingBudgetMs = 4.0f;

        /// <summary>
        /// Exact-match set of topic names treated as realtime (drained first,
        /// never deferred). Populate from Unity (e.g. "/tf", camera, scan, odom).
        /// Empty by default.
        /// </summary>
        public static readonly HashSet<string> RealtimeTopics = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>
        /// Optional additional predicate for classifying a topic as realtime,
        /// used when an exact-match in <see cref="RealtimeTopics"/> is not found
        /// (e.g. prefix rules). Null by default.
        /// </summary>
        public static Func<string, bool> RealtimeTopicPredicate = null;

        static bool IsRealtimeTopic(string topic)
        {
            if (RealtimeTopics.Count > 0 && RealtimeTopics.Contains(topic))
                return true;
            Func<string, bool> predicate = RealtimeTopicPredicate;
            if (predicate != null)
            {
                try
                {
                    return predicate(topic);
                }
                catch (Exception e)
                {
                    Debug.LogException(e);
                }
            }
            return false;
        }

        // Reused scratch buffer for the bulk remainder carried between frames.
        readonly List<Tuple<string, byte[]>> m_BulkCarryover = new List<Tuple<string, byte[]>>();

        void Update()
        {
            s_RealTimeSinceStartup = Time.realtimeSinceStartup;

            if (EnableRealtimeFirstPump)
            {
                UpdateRealtimeFirst();
                return;
            }

            Tuple<string, byte[]> data;
            while (m_IncomingMessages.TryDequeue(out data))
            {
                (string topic, byte[] contents) = data;
                m_LastMessageReceivedRealtime = Time.realtimeSinceStartup;

                if (m_SpecialIncomingMessageHandler != null)
                {
                    m_SpecialIncomingMessageHandler(topic, contents);
                }
                else if (topic.StartsWith("__"))
                {
                    ReceiveSysCommand(topic, Encoding.UTF8.GetString(contents));
                }
                else
                {
                    RosTopicState topicInfo = GetTopic(topic);
                    // if this is null, we have received a message on a topic we've never heard of...!?
                    // all we can do is ignore it, we don't even know what type it is
                    if (topicInfo != null)
                    {
                        try
                        {
                            //Add a try catch so that bad logic from one received message doesn't
                            //cause the Update method to exit without processing other received messages.
                            topicInfo.OnMessageReceived(contents);
                        }
                        catch (Exception e)
                        {
                            Debug.LogException(e);
                        }

                    }
                }
            }
        }

        // Dispatches a single already-dequeued message exactly as the stock loop
        // would. Kept identical in behavior so realtime/bulk lanes share one
        // code path and cannot diverge in handling.
        void DispatchIncoming(string topic, byte[] contents)
        {
            m_LastMessageReceivedRealtime = Time.realtimeSinceStartup;

            if (m_SpecialIncomingMessageHandler != null)
            {
                m_SpecialIncomingMessageHandler(topic, contents);
            }
            else if (topic.StartsWith("__"))
            {
                ReceiveSysCommand(topic, Encoding.UTF8.GetString(contents));
            }
            else
            {
                RosTopicState topicInfo = GetTopic(topic);
                if (topicInfo != null)
                {
                    try
                    {
                        topicInfo.OnMessageReceived(contents);
                    }
                    catch (Exception e)
                    {
                        Debug.LogException(e);
                    }
                }
            }
        }

        void UpdateRealtimeFirst()
        {
            // Carry-over bulk messages from previous frame(s) are already present
            // in m_BulkCarryover (FIFO) and are processed first in pass 2 so a
            // steady bulk stream cannot be starved.
            float budgetSeconds = Mathf.Max(0f, BulkProcessingBudgetMs) * 0.001f;
            float frameStart = Time.realtimeSinceStartup;

            // Snapshot this frame's freshly-arrived messages once, in order.
            // Realtime + syscommand + special-handler messages are dispatched
            // immediately; bulk data messages are appended to the carryover for
            // budgeted processing. If a special handler is pending or armed, we
            // MUST process strictly in order, so everything is dispatched inline.
            Tuple<string, byte[]> data;
            while (m_IncomingMessages.TryDequeue(out data))
            {
                string topic = data.Item1;

                bool mustStayInOrder = m_SpecialIncomingMessageHandler != null || topic.StartsWith("__");
                if (mustStayInOrder)
                {
                    // Service handshake / response routing and syscommands are
                    // strictly order-sensitive: dispatch immediately. Any bulk
                    // messages already buffered for this frame remain queued in
                    // m_BulkCarryover and keep their earlier-arrival ordering.
                    DispatchIncoming(data.Item1, data.Item2);
                }
                else if (IsRealtimeTopic(topic))
                {
                    DispatchIncoming(data.Item1, data.Item2);
                }
                else
                {
                    m_BulkCarryover.Add(data);
                }
            }

            // A syscommand can arm m_SpecialIncomingMessageHandler while its
            // paired payload has not arrived yet. Do not let older deferred
            // bulk data become the "next" special-handler payload; wait for
            // the actual next incoming message to satisfy the handler first.
            if (m_SpecialIncomingMessageHandler != null)
            {
                return;
            }

            // Pass 2: drain bulk messages under the per-frame time budget,
            // preserving order. Whatever does not fit stays in m_BulkCarryover
            // for the next Update(). A budget <= 0 still guarantees forward
            // progress by processing at least one bulk message per frame.
            int processed = 0;
            while (processed < m_BulkCarryover.Count)
            {
                Tuple<string, byte[]> bulk = m_BulkCarryover[processed];

                // If a service handshake arrives interleaved (its syscommand was
                // handled above and armed m_SpecialIncomingMessageHandler), the
                // next raw message must go to that handler, not here. Bulk data
                // never arms a handler, so this only guards against a handler
                // left pending across frames; dispatch in order to be safe.
                DispatchIncoming(bulk.Item1, bulk.Item2);
                processed++;

                if (budgetSeconds > 0f && Time.realtimeSinceStartup - frameStart >= budgetSeconds)
                {
                    // Budget spent; remaining bulk messages stay in
                    // m_BulkCarryover and are drained on subsequent frames.
                    break;
                }
            }

            // Compact the carryover: drop the processed prefix, keep the rest in
            // order for next frame.
            if (processed > 0)
            {
                if (processed >= m_BulkCarryover.Count)
                    m_BulkCarryover.Clear();
                else
                    m_BulkCarryover.RemoveRange(0, processed);
            }
        }

        float m_LastTopicsRequestRealtime = -1;
        const float k_TimeBetweenTopicsUpdates = 5.0f;

        public void RefreshTopicsList()
        {
            // cap the rate of requests
            if (m_LastTopicsRequestRealtime != -1 && s_RealTimeSinceStartup - m_LastTopicsRequestRealtime <= k_TimeBetweenTopicsUpdates)
                return;

            m_LastTopicsRequestRealtime = s_RealTimeSinceStartup;
            GetTopicAndTypeList((data) =>
            {
                foreach (KeyValuePair<string, string> kv in data)
                {
                    RosTopicState state = GetOrCreateTopic(kv.Key, kv.Value);
                    if (state.RosMessageName != kv.Value)
                    {
                        state.ChangeRosMessageName(kv.Value);
                    }
                }
            });
        }

        void ReceiveSysCommand(string topic, string json)
        {
            switch (topic)
            {
                case SysCommand.k_SysCommand_Handshake:
                    {
                        var handshakeCommand = JsonUtility.FromJson<SysCommand_Handshake>(json);
                        if (handshakeCommand.version == null)
                        {
                            Debug.LogError($"Corrupted or unreadable ROS-TCP-Endpoint version data! Expected: {k_Version}");
                        }
                        else if (!handshakeCommand.version.StartsWith(k_CompatibleVersionPrefix))
                        {
                            Debug.LogError($"Incompatible ROS-TCP-Endpoint version: {handshakeCommand.version}. Expected: {k_Version}");
                        }

                        var handshakeMetadata = JsonUtility.FromJson<SysCommand_Handshake_Metadata>(handshakeCommand.metadata);
#if ROS2
                        if (handshakeMetadata.protocol != "ROS2")
                        {
                            Debug.LogError($"Incompatible protocol: ROS-TCP-Endpoint is using {handshakeMetadata.protocol}, but Unity is in ROS2 mode. Switch it from the Robotics/Ros Settings menu.");
                        }
#else
                        if (handshakeMetadata.protocol != "ROS1")
                        {
                            Debug.LogError($"Incompatible protocol: ROS-TCP-Endpoint is using {handshakeMetadata.protocol}, but Unity is in ROS1 mode. Switch it from the Robotics/Ros Settings menu.");
                        }
#endif
                    }
                    break;
                case SysCommand.k_SysCommand_Log:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.Log(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_Warning:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.LogWarning(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_Error:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.LogError(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_ServiceRequest:
                    {
                        var serviceCommand = JsonUtility.FromJson<SysCommand_Service>(json);

                        // the next incoming message will be a request for a Unity service, so set a special callback to process it
                        m_SpecialIncomingMessageHandler = (string serviceTopic, byte[] requestBytes) =>
                        {
                            m_SpecialIncomingMessageHandler = null;

                            RosTopicState topicState = GetTopic(serviceTopic);
                            if (topicState == null)
                            {
                                Debug.LogError($"Unity service {serviceTopic} has not been implemented!");
                                return;
                            }

                            topicState.HandleUnityServiceRequest(requestBytes, serviceCommand.srv_id);
                        };
                    }
                    break;

                case SysCommand.k_SysCommand_ServiceResponse:
                    {
                        // the next incoming message will be a response from a ros service
                        var serviceCommand = JsonUtility.FromJson<SysCommand_Service>(json);
                        m_SpecialIncomingMessageHandler = (string serviceTopic, byte[] requestBytes) =>
                        {
                            m_SpecialIncomingMessageHandler = null;

                            TaskPauser resumer;
                            lock (m_ServiceRequestLock)
                            {
                                if (!m_ServicesWaiting.TryGetValue(serviceCommand.srv_id, out resumer))
                                {
                                    Debug.LogError($"Unable to route service response on \"{serviceTopic}\"! SrvID {serviceCommand.srv_id} does not exist.");
                                    return;
                                }

                                m_ServicesWaiting.Remove(serviceCommand.srv_id);
                            }
                            resumer.Resume(requestBytes);
                        };
                    }
                    break;

                case SysCommand.k_SysCommand_TopicList:
                    {
                        var topicsResponse = JsonUtility.FromJson<SysCommand_TopicsResponse>(json);
                        if (m_TopicsAndTypesListCallbacks.Count > 0)
                        {
                            Dictionary<string, string> callbackParam = new Dictionary<string, string>();
                            for (int idx = 0; idx < topicsResponse.topics.Length; ++idx)
                                callbackParam[topicsResponse.topics[idx]] = topicsResponse.types[idx];
                            m_TopicsAndTypesListCallbacks.ForEach(a => a(callbackParam));
                            m_TopicsAndTypesListCallbacks.Clear();
                        }
                        if (m_TopicsListCallbacks.Count > 0)
                        {
                            m_TopicsListCallbacks.ForEach(a => a(topicsResponse.topics));
                            m_TopicsListCallbacks.Clear();
                        }
                    }
                    break;
            }
        }

        static void SendKeepalive(NetworkStream stream)
        {
            // 8 zeroes = a ros message with topic "" and no message data.
            stream.Write(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0, 8);
        }

        static void ClearMessageQueue(OutgoingMessageQueue queue)
        {
            while (queue.TryDequeue(out OutgoingMessageSender sendsOutgoingMessages))
            {
                sendsOutgoingMessages.ClearAllQueuedData();
            }
        }

        static async Task ConnectionThread(
            string rosIPAddress,
            int rosPort,
            float networkTimeoutSeconds,
            float keepaliveTime,
            int sleepMilliseconds,
            Action<NetworkStream> OnConnectionStartedCallback,
            Action DeregisterAll,
            OutgoingMessageQueue outgoingQueue,
            ConcurrentQueue<Tuple<string, byte[]>> incomingQueue,
            CancellationToken token)
        {
            //Debug.Log("ConnectionThread begins");
            int nextReaderIdx = 101;
            int nextReconnectionDelay = 1000;
            MessageSerializer messageSerializer = new MessageSerializer();

            while (!token.IsCancellationRequested)
            {
                TcpClient client = null;
                CancellationTokenSource readerCancellation = null;

                try
                {
                    ROSConnection.m_HasConnectionError = true; // until we actually see a reply back, assume there's a problem

                    client = new TcpClient();
                    client.Connect(rosIPAddress, rosPort);

                    NetworkStream networkStream = client.GetStream();
                    networkStream.ReadTimeout = (int)(networkTimeoutSeconds * 1000);

                    SendKeepalive(networkStream);
                    OnConnectionStartedCallback(networkStream);

                    readerCancellation = new CancellationTokenSource();
                    _ = Task.Run(() => ReaderThread(nextReaderIdx, networkStream, incomingQueue, sleepMilliseconds, readerCancellation.Token));
                    nextReaderIdx++;

                    // connected, now just watch our queue for outgoing messages to send (or else send a keepalive message occasionally)
                    float waitingSinceRealTime = s_RealTimeSinceStartup;
                    while (true)
                    {
                        bool messageReadyEventWasSet = outgoingQueue.NewMessageReadyToSendEvent.WaitOne(sleepMilliseconds);
                        token.ThrowIfCancellationRequested();

                        if (messageReadyEventWasSet)
                        {
                            outgoingQueue.NewMessageReadyToSendEvent.Reset();
                        }
                        else
                        {
                            if (s_RealTimeSinceStartup > waitingSinceRealTime + keepaliveTime)
                            {
                                SendKeepalive(networkStream);
                                waitingSinceRealTime = s_RealTimeSinceStartup;
                            }
                        }

                        while (outgoingQueue.TryDequeue(out OutgoingMessageSender sendsOutgoingMessages))
                        {
                            OutgoingMessageSender.SendToState sendToState = sendsOutgoingMessages.SendInternal(messageSerializer, networkStream);
                            switch (sendToState)
                            {
                                case OutgoingMessageSender.SendToState.Normal:
                                    //This is normal operation.
                                    break;
                                case OutgoingMessageSender.SendToState.QueueFullWarning:
                                    //Unable to send messages to ROS as fast as we're generating them.
                                    //This could be caused by a TCP connection that is too slow.
                                    Debug.LogWarning($"Queue full! Messages are getting dropped! " +
                                                     "Try check your connection speed is fast enough to handle the traffic.");
                                    break;
                                case OutgoingMessageSender.SendToState.NoMessageToSendError:
                                    //This indicates
                                    Debug.LogError(
                                        "Logic Error! A 'SendsOutgoingMessages' was queued but did not have any messages to send.");
                                    break;
                            }

                            token.ThrowIfCancellationRequested();
                            waitingSinceRealTime = s_RealTimeSinceStartup;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception e)
                {
                    ROSConnection.m_HasConnectionError = true;
                    Debug.Log($"Connection to {rosIPAddress}:{rosPort} failed - " + e);
                    await Task.Delay(nextReconnectionDelay);
                }
                finally
                {
                    if (readerCancellation != null)
                        readerCancellation.Cancel();

                    if (client != null)
                        client.Close();

                    // clear the message queue
                    ClearMessageQueue(outgoingQueue);
                    DeregisterAll();
                }
                await Task.Yield();
            }
        }

        static async Task ReaderThread(int readerIdx, NetworkStream networkStream, ConcurrentQueue<Tuple<string, byte[]>> queue, int sleepMilliseconds, CancellationToken token)
        {
            // First message should be the handshake
            Tuple<string, byte[]> handshakeContent = await ReadMessageContents(networkStream, sleepMilliseconds, token);
            if (handshakeContent.Item1 == SysCommand.k_SysCommand_Handshake)
            {
                ROSConnection.m_HasConnectionError = false;
                queue.Enqueue(handshakeContent);
            }
            else
            {
                Debug.LogError($"Invalid ROS-TCP-Endpoint version detected: 0.6.0 or older. Expected: {k_Version}.");
            }

            while (!token.IsCancellationRequested)
            {
                try
                {
                    Tuple<string, byte[]> content = await ReadMessageContents(networkStream, sleepMilliseconds, token);
                    ROSConnection.m_HasConnectionError = false;

                    if (content.Item1 != "") // ignore keepalive messages
                        queue.Enqueue(content);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception e)
                {
                    ROSConnection.m_HasConnectionError = true;
                    Debug.Log("Reader " + readerIdx + " exception! " + e);
                }
            }
        }

        static async Task ReadToByteArray(NetworkStream networkStream, byte[] array, int length, int sleepMilliseconds, CancellationToken token)
        {
            int read = 0;
            while (read < length && networkStream.CanRead)
            {
                while (!token.IsCancellationRequested && !networkStream.DataAvailable)
                    await Task.Delay(sleepMilliseconds);

                token.ThrowIfCancellationRequested();
                read += await networkStream.ReadAsync(array, read, length - read, token);
            }

            if (read < length)
                throw new SocketException(); // the connection has closed
        }

        static byte[] s_FourBytes = new byte[4];
        static byte[] s_TopicScratchSpace = new byte[64];

        static async Task<Tuple<string, byte[]>> ReadMessageContents(NetworkStream networkStream, int sleepMilliseconds, CancellationToken token)
        {
            // Get first bytes to determine length of topic name
            await ReadToByteArray(networkStream, s_FourBytes, 4, sleepMilliseconds, token);
            int topicLength = BitConverter.ToInt32(s_FourBytes, 0);

            // If our topic buffer isn't large enough, make a larger one (and keep it that size; assume that's the new standard)
            if (topicLength > s_TopicScratchSpace.Length)
                s_TopicScratchSpace = new byte[topicLength];

            // Read and convert topic name
            await ReadToByteArray(networkStream, s_TopicScratchSpace, topicLength, sleepMilliseconds, token);
            string topicName = Encoding.ASCII.GetString(s_TopicScratchSpace, 0, topicLength);

            await ReadToByteArray(networkStream, s_FourBytes, 4, sleepMilliseconds, token);
            int full_message_size = BitConverter.ToInt32(s_FourBytes, 0);

            byte[] readBuffer = new byte[full_message_size];
            await ReadToByteArray(networkStream, readBuffer, full_message_size, sleepMilliseconds, token);

            return Tuple.Create(topicName, readBuffer);
        }

        void OnApplicationQuit()
        {
            Disconnect();
        }

        void SendSysCommand(string command, object param, NetworkStream stream = null)
        {
            if (stream != null)
                SendSysCommandImmediate(command, param, stream);
            else
                QueueSysCommand(command, param);
        }

        static void PopulateSysCommand(MessageSerializer messageSerializer, string command, object param)
        {
            messageSerializer.Clear();
            // syscommands are sent as:
            // 4 byte command length, followed by that many bytes of the command
            // (all command names start with __ to distinguish them from ros topics)
            messageSerializer.Write(command);
            // 4-byte json length, followed by a json string of that length
            string json = JsonUtility.ToJson(param);
            messageSerializer.WriteUnaligned(json);
        }

        static void SendSysCommandImmediate(string command, object param, NetworkStream stream)
        {
            if (stream == null)
            {
                throw new ArgumentException("stream cannot be null!");
            }
            MessageSerializer messageSerializer = new MessageSerializer();
            PopulateSysCommand(messageSerializer, command, param);
            messageSerializer.SendTo(stream);
        }

        public void QueueSysCommand(string command, object param)
        {
            PopulateSysCommand(m_MessageSerializer, command, param);
            m_OutgoingMessageQueue.Enqueue(new SysCommandSender(m_MessageSerializer.GetBytesSequence()));
        }

        [Obsolete("Use Publish instead of Send", false)]
        public void Send(string rosTopicName, Message message)
        {
            Publish(rosTopicName, message);
        }

        public void Publish(string rosTopicName, Message message)
        {
            if (rosTopicName.StartsWith("__"))
            {
                QueueSysCommand(rosTopicName, message);
            }
            else
            {
                RosTopicState rosTopic = GetTopic(rosTopicName);
                if (rosTopic == null || !rosTopic.IsPublisher)
                {
                    throw new Exception($"No registered publisher on topic {rosTopicName} for type {message.RosMessageName}!");
                }

                rosTopic.Publish(message);
            }
        }

        void InitializeHUD()
        {
            if (!Application.isPlaying || (!m_ShowHUD && m_HudPanel == null))
                return;

            if (m_HudPanel == null)
            {
                m_HudPanel = gameObject.AddComponent<HudPanel>();
            }

            m_HudPanel.isEnabled = m_ShowHUD;
        }

        void DrawHeaderGUI()
        {
            GUIStyle labelStyle = new GUIStyle
            {
                alignment = TextAnchor.MiddleLeft,
                normal = { textColor = Color.white },
                fontStyle = FontStyle.Bold,
                fixedWidth = 250
            };

            GUIStyle contentStyle = new GUIStyle
            {
                alignment = TextAnchor.MiddleLeft,
                padding = new RectOffset(10, 0, 0, 5),
                normal = { textColor = Color.white },
                fixedWidth = 300
            };


            // ROS IP Setup
            GUILayout.BeginHorizontal();
            DrawConnectionArrows(
                true,
                0,
                0,
                Time.realtimeSinceStartup - LastMessageReceivedRealtime,
                Time.realtimeSinceStartup - LastMessageSentRealtime,
                HasConnectionThread,
                HasConnectionThread,
                HasConnectionError
            );

#if ROS2
            string protocolName = "ROS2";
#else
            string protocolName = "ROS";
#endif

            GUILayout.Space(30);
            GUILayout.Label($"{protocolName} IP: ", labelStyle, GUILayout.Width(100));

            if (!HasConnectionThread)
            {
                // if you've never run a build on this machine before, initialize the playerpref settings to the ones from the RosConnection
                if (!PlayerPrefs.HasKey(PlayerPrefsKey_ROS_IP))
                    SetIPPref(RosIPAddress);
                if (!PlayerPrefs.HasKey(PlayerPrefsKey_ROS_TCP_PORT))
                    SetPortPref(RosPort);

                // NB, here the user is editing the PlayerPrefs values, not the ones in the RosConnection.
                // (So that the hud remembers what IP you used last time you ran this build.)
                // The RosConnection receives the edited values when you click Connect.
                SetIPPref(GUILayout.TextField(RosIPAddressPref));
                SetPortPref(Convert.ToInt32(GUILayout.TextField(RosPortPref.ToString())));

                GUILayout.EndHorizontal();
                GUILayout.Label("(Not connected)");
                if (GUILayout.Button("Connect"))
                    Connect(RosIPAddressPref, RosPortPref);
            }
            else
            {
                GUILayout.Label($"{RosIPAddress}:{RosPort}", contentStyle);
                GUILayout.EndHorizontal();
            }
        }

        static GUIStyle s_ConnectionArrowStyle;

        public static void DrawConnectionArrows(bool withBar, float x, float y, float receivedTime, float sentTime, bool isPublisher, bool isSubscriber, bool hasError)
        {
            if (s_ConnectionArrowStyle == null)
            {
                s_ConnectionArrowStyle = new GUIStyle
                {
                    alignment = TextAnchor.MiddleLeft,
                    normal = { textColor = Color.white },
                    fontSize = 22,
                    fontStyle = FontStyle.Bold,
                    fixedWidth = 250
                };
            }

            var baseColor = GUI.color;
            GUI.color = Color.white;
            if (withBar)
                GUI.Label(new Rect(x + 4, y + 5, 25, 15), "I", s_ConnectionArrowStyle);
            GUI.color = GetConnectionColor(receivedTime, isSubscriber, hasError);
            GUI.Label(new Rect(x + 8, y + 6, 25, 15), "\u2190", s_ConnectionArrowStyle);
            GUI.color = GetConnectionColor(sentTime, isPublisher, hasError);
            GUI.Label(new Rect(x + 8, y + 0, 25, 15), "\u2192", s_ConnectionArrowStyle);
            GUI.color = baseColor;
        }

        public static void SetIPPref(string ipAddress)
        {
            PlayerPrefs.SetString(PlayerPrefsKey_ROS_IP, ipAddress);
        }

        public static void SetPortPref(int port)
        {
            PlayerPrefs.SetInt(PlayerPrefsKey_ROS_TCP_PORT, port);
        }

        public static Color GetConnectionColor(float elapsedTime, bool hasConnection, bool hasError)
        {
            var bright = new Color(1, 1, 0.5f);
            var mid = new Color(0, 1, 1);
            var dark = new Color(0, 0.5f, 1);
            const float brightDuration = 0.03f;
            const float fadeToDarkDuration = 1.0f;

            if (!hasConnection)
                return Color.gray;
            if (hasError)
                return Color.red;

            if (elapsedTime <= brightDuration)
                return bright;
            return Color.Lerp(mid, dark, elapsedTime / fadeToDarkDuration);
        }

        public static bool IPFormatIsCorrect(string ipAddress)
        {
            if (ipAddress == null || ipAddress == "")
                return false;

            // If IP address is set using static lookup tables https://man7.org/linux/man-pages/man5/hosts.5.html
            if (Char.IsLetter(ipAddress[0]))
            {
                foreach (Char subChar in ipAddress)
                {
                    if (!(Char.IsLetterOrDigit(subChar) || subChar == '-' || subChar == '.'))
                        return false;
                }

                if (!Char.IsLetterOrDigit(ipAddress[ipAddress.Length - 1]))
                    return false;
                return true;
            }

            string[] subAdds = ipAddress.Split('.');
            if (subAdds.Length != 4)
            {
                return false;
            }
            IPAddress parsedipAddress;
            return IPAddress.TryParse(ipAddress, out parsedipAddress);
        }
    }
}
