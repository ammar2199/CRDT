#include <algorithm>
#include <array>
#include <sys/socket.h>
#include <cstdio>
#include <unistd.h>
#include <cstdint>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <iostream>
#include <limits>
#include <sys/ioctl.h>
#include <unordered_map>
#include <cassert>
#include <termios.h>
#include <list>
#include <fcntl.h>
#include <fstream>
#include <cstring>
#include <signal.h>
#include <memory>
#include <poll.h>

// Original Paper:
// http://csl.skku.edu/papers/jpdc11.pdf

// TODO: 
// - Add Tombstone Purging
// - Add delay before we send so it's easier to test concurrent operations.
//   so I can singlehandely test concurrent operations. 
// - Cleanup RGA implementation, move to separate file.
// - Test across more than 2 machines.
//      - Will need to update VectorClock class.
// - Fix Cursor-Management Issues:
//    - Cursor needs to be appropriately modified
//      when a remote operation occurs so that colloborative
//      text editing is seamless.
//      You pretty much only modify the cursor when the cobject/index
//      to the inserted/deleted element is before it.
//  - Still some weird issue when socket disconnect happens 
//    on client side. Might've happened because I switched
//    from POLLRDHUP to POLLHUP.

static const char* IPAddress = "127.0.0.1";
static constexpr uint16_t PortNumber = 2500;
static bool isClient = false;
static const char* ptsName = NULL;

std::unique_ptr<std::basic_streambuf<char>> fileBuffer;
std::unique_ptr<std::ostream> logoutPtr;
#define logout (*logoutPtr)

#define CHECK_ERR(res, string)  \
  if (res == -1) {  \
    perror(string); \
    return -1;      \
  }                 \

struct S4Vector { // XXX: Only use [siteId, sum] for now...
  S4Vector() {
    mVector[0] = -1;
  }

  S4Vector(const int site, const int sum) {
    mVector[0] = sum;
    mVector[1] = site;
  }
  
  bool operator<(const S4Vector& other) {
    // lexicographic comparison
    return mVector < other.mVector;
  }

  bool IsValid() const {
    return mVector[0] != -1;
  }

  bool operator==(const S4Vector& other) const {
    return (mVector[0] == other.mVector[0]) && (mVector[1] == other.mVector[1]);
  }

  int GetSiteId() const {
    return mVector[1];
  }

  int GetSum() const {
    return mVector[0];
  }
 private:
  std::array<int, 2> mVector;
};

std::ostream& operator<<(std::ostream& os, const S4Vector& vector) {
  if (!vector.IsValid()) {
    os << "S4Vector[Invalid]";
    return os;
  }
  os << "S4Vector[Site:" << vector.GetSiteId() << "," << vector.GetSum() << "]";
  return os;
}


class VectorClock {
 public:
 VectorClock(const int siteId) : mSiteId(siteId) {
   assert(siteId < mCounter.size());
   for (int i=0; i<mCounter.size(); i++) mCounter[i] = 0;
 }

 void Increment() {
   mCounter[mSiteId]++;
 }

 void Decrement() {
   mCounter[mSiteId]--;
 }

 S4Vector CreateS4Vector() const {
  int sum = 0;
  for (int i=0; i<mCounter.size(); i++) sum += mCounter[i];
  return S4Vector(mSiteId, sum);
 }

 void Merge(const VectorClock& otherClock) {
  for (int i=0; i<mCounter.size(); i++) {
    mCounter[i] = std::max<int>(mCounter[i], otherClock.mCounter[i]); 
  }
 }
  
 int GetSiteValue(const int i) const {
   assert(i < mCounter.size());
   return mCounter[i];
 }

 void SetSiteValue(int idx, int val) {
   assert(idx < mCounter.size());
   mCounter[idx] = val;
 }

 int GetNumSites() const {
   return mCounter.size();
 }

 friend std::ostream& operator<<(std::ostream& out, const VectorClock& clock);

 private:
 int mSiteId;
 std::array<int, 2> mCounter;
};

std::ostream& operator<<(std::ostream& out, const VectorClock& clock) {
  out << "VectorClock of Site " << clock.mSiteId;
  out << ": [";
  for (int i=0; i<clock.GetNumSites(); i++) {
    out << clock.GetSiteValue(i);
    if (i != clock.GetNumSites() - 1) out << ',';
  }
  out << ']';
  return out;
}


// Use 0 to represent tombstone.
//
struct RGANode {
  char object;
  S4Vector vectorHash;
  S4Vector deleteVector;
  RGANode* link; 
};

namespace std {
template<>
struct hash<S4Vector> {
  std::size_t fnv1a(int x) const {
    std::size_t hash = 2166196261u;
    for (int i=0; i < 4; i++) {
      hash ^= (x >> (i * 8)) & 0xFF;
      hash *= 16777619u;
    }
    return hash;
  }

  std::size_t operator()(const S4Vector& vector) const {
    std::size_t h1 = fnv1a(vector.GetSiteId());
    std::size_t h2 = fnv1a(vector.GetSum());
    return h1 ^ (h2 << 1);
  }
};
} // namespace std


// Notes:
// -   RGA List is 0-indexed.
// -   No tombstone-purging yet...
class RGAList {
 public:
  RGAList(VectorClock& vectorClock) : mClockRef(vectorClock), mListHead(nullptr), mLength(0) {}
  
  bool LocalInsert(const int index, char c, S4Vector& leftCobject) {
    // RGA is 0-indexed. index must be between [0, mLength].
    // When we insert at index i, the currently occupied item
    // get's shifted over to the right.
    assert(c != 0 && "Can't Insert 0-Valued Character");
    assert(index >= 0 && index <= mLength && "Index must be less than or equal to Array Length");  

    // Create new Node
    RGANode* addNode = new RGANode();
    if (!addNode) return false;

    addNode->object = c;
    addNode->vectorHash = mClockRef.CreateS4Vector();
    addNode->deleteVector = addNode->vectorHash;
    mObjectMap[addNode->vectorHash] = addNode; 

    if (!mListHead || index == 0) {
      if (index != 0) {
        throw std::runtime_error("Out of Range - index > 0, while no Head");
      }

      // Insert at head.
      addNode->link = mListHead;
      mListHead = addNode;
      leftCobject = S4Vector(); // Invalid S4Vector.
      mLength++;
      return true;
    }
    
    RGANode* iter = mListHead;
    int count = 0;
    while (iter) {
      if (iter->object != 0) {
        count++;
        if (count == index) break;
      }
      iter = iter->link;
    }
    
    // Now iter points to RGANode which contains leftCobject. 
    // We need to insert the new node after iter.
    addNode->link = iter->link;
    iter->link = addNode;
    leftCobject = iter->vectorHash;
    mLength++;
    return true;
  };
  
  // Delete RGA node at index 'index'
  // index e [0, mLength-1]
  // 'cobject' is set to the cobject of the deleted node.
  bool LocalDelete(const int index, S4Vector& cobject) {
    assert(index >= 0 && index < GetLength() && "Index must be less than length");
    RGANode* iter = mListHead;
    
    int count = 0; 
    while (iter) {
      if (iter->object != 0) { 
        count++;
        if (count > index) break;
      }
      iter = iter->link;
    }

    iter->object = 0; // Set as tombstone.
    cobject = iter->vectorHash;
    mLength--;
    return true;
  }

  bool RemoteDelete(const S4Vector& cobject, const S4Vector& operationVector) {
    // Because we NEVER delete nodes, we should always be able to find it.
    auto iter = mObjectMap.find(cobject);
    if (iter == mObjectMap.end()) {
      throw std::runtime_error("RemoteDelete: Failed to find cobject");
    }
    RGANode* node = iter->second;
    if (node->object != 0) {
      node->object = 0; // tombstone
      node->deleteVector = operationVector;  
      mLength--;
    }
    return true;
  }

  std::string GetString() const {
    std::string result;
    const RGANode* iter = mListHead;
    while (iter) {
      if (iter->object) {
        result += iter->object;
      }
      iter = iter->link;
    }
    return result;
  }
  
  // Receive S4Vector and character to insert
  //
  // s4Op is the S4 vector for this operation. Which was 
  // generated when the Local Insert was called at the corresponding site and tx'd here.
  bool RemoteInsert(const S4Vector& cobject, const S4Vector& s4Op, const char c) {
    RGANode* ref = nullptr;
    if (cobject.IsValid()) {
      auto iter = mObjectMap.find(cobject);
      if (iter == mObjectMap.end()) {
        throw std::runtime_error("Failed to find cobject using S4Vector");
      }
      ref = iter->second;
    }
    
    mLength++;
    RGANode* insert = new RGANode;
    insert->object = c;
    insert->vectorHash = s4Op;
    insert->deleteVector = s4Op;
    mObjectMap[s4Op] = insert;
    
    if (!cobject.IsValid()) {
      if (!mListHead || mListHead->vectorHash < insert->vectorHash) {
        insert->link = mListHead;
        mListHead = insert;
        return true;
      }
      ref = mListHead;
    }
    
    while (ref->link && (insert->vectorHash < ref->link->vectorHash)) {
      ref = ref->link;
    }
    insert->link = ref->link;
    ref->link = insert;
    return true;
  }

  size_t GetLength() const {
    return mLength;
  }
 private: 
  const VectorClock& mClockRef;
  RGANode* mListHead;
  std::unordered_map<S4Vector, RGANode*> mObjectMap; // For fast causal-object lookup.
  size_t mLength;
};

// Message Structure
// 1. SiteId (1 Byte)
// 2. Operation Info (1 Byte)
//    - Insert : index-cobject (which will be the left-cobject)
//    - Delete : index-cobject (cobject to remove)
//    - Update : index-cobject (cobject to update)
// 3. character: (if Operation is insert)
// 3. vector-clock of operation (NumSites * 4bytes) (8 bytes in our case)

#pragma pack(1) // no padding
struct Message {
  Message() = default;
  Message(uint8_t siteId, uint8_t opcode, const S4Vector& cobject, VectorClock& vectorClock) {
    mSiteId = siteId;
    mOpCode = opcode;
    mClock[0] = vectorClock.GetSiteValue(0);
    mClock[1] = vectorClock.GetSiteValue(1);
    mOpInfo[0] = cobject.GetSiteId();
    mOpInfo[1] = cobject.GetSum();
  }

  void SetCharacter(char c) {
    assert(mOpCode == Message::OP_INSERT);
    mChar = c;
  }

  char GetCharacter() const {
    assert(mOpCode == Message::OP_INSERT);
    return mChar;
  }

  S4Vector GetCobjectVector() const {
    return S4Vector(mOpInfo[0], mOpInfo[1]);
  }
  
  S4Vector BuildOpS4Vector() const {
    int sum = 0;
    for (int i=0; i<mClock.size(); i++) sum += mClock[i];
    return S4Vector(mSiteId, sum); 
  }

  bool IsReady(const VectorClock& siteClock) const {
    for (int i=0; i<siteClock.GetNumSites(); i++) {
      if (i == mSiteId) {
       if (mClock[i] != siteClock.GetSiteValue(i) + 1) return false;
      } else {
        if (mClock[i] > siteClock.GetSiteValue(i)) return false;
      }
    }
    return true;
  }

  uint8_t GetOperation() const {
    return mOpCode;
  }

  // GetClock
  VectorClock GetOperationVectorClock() const {
    VectorClock clock(mSiteId);
    for (int i=0; i<mClock.size(); i++) {
      clock.SetSiteValue(i, mClock[i]);
    }
    return clock;
  }

  friend std::ostream& operator<<(std::ostream& out, const Message& m);

  static constexpr uint8_t OP_INSERT = 1;
  static constexpr uint8_t OP_DELETE = 2;
  uint8_t mSiteId; // 1 bytes
  uint8_t mOpCode; // 2 bytes
  std::array<uint32_t, 2> mOpInfo;  // 11 bytes Note: Redundantly contains siteId but whatev
  uint8_t mChar; // 12 bytes
  std::array<uint32_t, 2> mClock; // 20 bytes // XXX: Rename to operation clock, and 
                                              //      use contexpr or template-param
                                              //      instead of 2.
};
#pragma pack()

std::ostream& operator<<(std::ostream& out, const Message& m) {
  out << "Message from Site " << static_cast<int>(m.mSiteId) << ":\n";
  out << "Operation: ";
  if (m.GetOperation() == Message::OP_INSERT) {
    out << "Insert\n";
    out << "Character: " << m.GetCharacter() << '\n';
    out << "Index: " << m.GetCobjectVector()<< '\n';
    out << "Operation-Clock: " << m.GetOperationVectorClock() << '\n';
  } else if (m.GetOperation() == Message::OP_DELETE) {
    out << "Delete\n";
    out << "Index: " << m.GetCobjectVector() << '\n';
    out << "Operation-Clock: " << m.GetOperationVectorClock() << '\n';
  }
  return out;
}

class MessageQueue {
 public:
  void AddMessage(const Message& m) {
    mMessages.push_back(m);
  }

  bool ContainsReadyMessage(const VectorClock& clock) {
    return GetReadyMessage(clock, nullptr);
  }
  
  Message GetReadyMessage(const VectorClock& clock) {
    Message m;
    GetReadyMessage(clock, &m);
    return m;
  }

 private:

  bool GetReadyMessage(const VectorClock& clock, Message* out) {
    for (const auto& message : mMessages) {
      if (message.IsReady(clock)) {
        if (out) *out = message;
        return true;
      }
    }
    return false;
  }
  std::list<Message> mMessages;
};

class Terminal {
 public:
    Terminal() = default;

    void Init() {
      ioctl(STDOUT_FILENO, TIOCGWINSZ, &window);
      logout << "Window Size(RowxCol):" << window.ws_row << "x" << window.ws_col << std::endl;
      tcgetattr(STDIN_FILENO, &defaultTerminal);
    }

    void SetRawMode() const {
      struct termios rawTerm;
      rawTerm = defaultTerminal;
      rawTerm.c_lflag &= ~(ECHO | ICANON);
      tcsetattr(STDIN_FILENO, TCSAFLUSH, &rawTerm);
    }

    void SetDefaultMode() {
      tcsetattr(STDIN_FILENO, TCSAFLUSH, &defaultTerminal);
    }
    

    void ResetCursor() const {
      const char* cmd = "\033[H";
      write(STDOUT_FILENO, cmd, strlen(cmd));
    }

    void ClearScreen() const {
      const char* cmd = "\033[2J";
      write(STDOUT_FILENO, cmd, strlen(cmd));
    }

    void OutString(const std::string& str) const {
      write(STDOUT_FILENO, str.c_str(), str.length());
    }

    void SetCursorPosition(int strPosition) const {
      const char* cmd = "\033[%d;%dH";
      int row = (strPosition / window.ws_col) + 1;
      int col = (strPosition %  window.ws_col) + 1;
      char buf[20];
      int bytes = snprintf(buf, sizeof(buf), cmd, row, col);
      write(STDOUT_FILENO, buf, bytes);
    }
    
    void SetCursorPosition(int row, int col) const {
      const char* cmd = "\033[%d;%dH";
      char buf[20];
      int bytes = snprintf(buf, sizeof(buf), cmd, row, col);
      write(STDOUT_FILENO, buf, bytes); 
    }

    int GetNumRows() const {
      return window.ws_row;
    }

    int GetNumColumns() const {
      return window.ws_col;
    }

 private:
  struct winsize window;
  struct termios defaultTerminal;
};


static Terminal term;

const char* ArrowUp = "\033[A";
const char* ArrowDown = "\033[B";
const char* ArrowRight = "\033[C";
const char* ArrowLeft = "\033[D";

enum class InputType {
  Character,
  Backspace,
  ArrowLeft,
  ArrowRight,
  Unknown
};


static InputType GetInput(const void* buffer, unsigned int length, char& outChar) {
  outChar = 0;
  if (length == 1) {
    if (*(uint8_t*)buffer == 127) {
      return InputType::Backspace;
    } else if (*(uint8_t*)buffer >= 32) {
      outChar = ((char*)(buffer))[0];
      return InputType::Character;
    }
  } else if (length == 3) {
    if (memcmp(buffer, ArrowRight, length) == 0) {
      return InputType::ArrowRight;
    } 
    else if (memcmp(buffer, ArrowLeft, length) == 0) {
      return InputType::ArrowLeft;
    }
  }
  return InputType::Unknown;
}

static int sockFD = -1;
static int connectionSocketFD = -1;

// TODO: Write out RGA-String when we exit.
void signal_handler(int i) {
  close(sockFD);
  close(connectionSocketFD);
  term.ResetCursor();
  term.ClearScreen();
  term.SetDefaultMode();
}

int MainLoop(const int socketFD) {
  logout << "Entering Main Loop" << std::endl;
  signal(SIGINT, signal_handler);
  int SiteId = isClient ? 1 : 0;
  
  VectorClock siteClock(SiteId);
  std::array<char, 100> InputBuffer;

  struct pollfd events[2];

  // local event
  events[0].fd = STDIN_FILENO;
  events[0].events = POLLIN;

  // remote event
  events[1].fd = socketFD;
  events[1].events = POLLIN | POLLHUP; // POLLRDHUP seems to work
                                       // better but is Linux-specific.
                                       // They do slightly different 
                                       // things.

  // String RGA
  RGAList replicaString(siteClock);

  MessageQueue messageQueue;
  
  term.SetRawMode();
  
  int CursorPosition = 0; // Relative to the String

  // Input reading 
  while (true) { // while (event != EVENT_QUIT)
    bool remoteEventOccurred = false;
    int res = poll(events, sizeof(events)/sizeof(events[0]), -1);
    if (res == -1) {
      exit(1);
    }

    if ((events[1].revents & POLLHUP)) { // socket disconnect. Break loop
      break;
    }

    // character and array keys for cursor movement.
    if ((events[0].revents & POLLIN)) { // A local operation occured
      ssize_t bytesRead = read(STDIN_FILENO, InputBuffer.data(), 
                               InputBuffer.size() * sizeof(decltype(InputBuffer)::value_type));
      char inChar;
      InputType inType = GetInput(InputBuffer.data(), bytesRead, inChar);
      if (inType == InputType::ArrowRight) {
        CursorPosition = std::clamp<int>(++CursorPosition, 0, replicaString.GetLength());
        logout << "Cursor Position:" << CursorPosition << std::endl;
      } else if (inType == InputType::ArrowLeft) {
        CursorPosition = std::clamp<int>(--CursorPosition, 0, replicaString.GetLength());
        logout << "Cursor Position" << CursorPosition << std::endl;
      } else if (inType == InputType::Character) {
        // Local Operation Occurred!
        siteClock.Increment(); 
        S4Vector cobject;
        if (replicaString.LocalInsert(CursorPosition, inChar, cobject)) {
          // Build and Broadcast Message
          Message message(SiteId, Message::OP_INSERT, cobject, siteClock);
          message.SetCharacter(inChar);
          send(socketFD, &message, sizeof(Message), 0);
          logout << "Sent Message:\n" << message << std::endl;
          CursorPosition = std::clamp<int>(++CursorPosition, 0, replicaString.GetLength());
        } else {
          siteClock.Decrement();
        }
      } else if (inType == InputType::Backspace) {
        if (CursorPosition == 0) continue; // We want to delete characters to left of cursor
        
        // Local Delete Operation
        siteClock.Increment();
        S4Vector cobject;
        if (replicaString.LocalDelete(CursorPosition-1, cobject)) {
          Message message(SiteId, Message::OP_DELETE, cobject, siteClock);
          send(socketFD, &message, sizeof(Message), 0);
          CursorPosition = std::clamp<int>(--CursorPosition, 0, replicaString.GetLength());
        } else {
          siteClock.Decrement();
        }
      }
    }
    else if ((events[1].revents & POLLIN)) { // Remote Operation
      Message message;
      ssize_t inCnt = recv(socketFD, &message, sizeof(Message), 0);
      logout << "Recieved Message " << message << std::endl;
      messageQueue.AddMessage(message);

      if (!messageQueue.ContainsReadyMessage(siteClock)) {
        logout << "Message Received: But No Ready Messages" << std::endl;
      }

      while (messageQueue.ContainsReadyMessage(siteClock)) {
        Message readyMsg = messageQueue.GetReadyMessage(siteClock);
        logout << "Executing Message " << readyMsg << std::endl;
        siteClock.Merge(readyMsg.GetOperationVectorClock());
        switch (readyMsg.GetOperation()) {
          case Message::OP_INSERT:
            replicaString.RemoteInsert(readyMsg.GetCobjectVector(), readyMsg.BuildOpS4Vector(), readyMsg.GetCharacter());
            break;
          case Message::OP_DELETE:
            replicaString.RemoteDelete(readyMsg.GetCobjectVector(), readyMsg.BuildOpS4Vector());
            break;
          default:
            break;
        }
        CursorPosition = std::clamp<int>(CursorPosition, 0, replicaString.GetLength());
      }
    }

    // Terminal Output  
    // Reset
    term.ResetCursor();
    term.ClearScreen();
    
    // Output String
    term.OutString(replicaString.GetString());

    // Output Message for Logs
    term.SetCursorPosition(term.GetNumRows(), 0);
    term.OutString(std::string("Logs at: ") + ptsName);
    
    // Set Cursor Position
    term.SetCursorPosition(CursorPosition);
  }

  term.ResetCursor();
  term.ClearScreen();
  term.SetDefaultMode();
  return 0;
}

class FileBuf : public std::basic_streambuf<char> {
 public:
  
  using int_type = std::basic_streambuf<char>::int_type;
  using CharTraits = std::char_traits<char>;

  FileBuf(int fd) : std::basic_streambuf<char>(), mFd(fd) {
    assert(mFd >= 0);
  }

  virtual ~FileBuf() {
    close(mFd);
  }

  virtual std::streamsize xsputn(const char* buf, std::streamsize count) override {
    int res = write(mFd, buf, count);
    return res > 0 ? res : 0;
  }
  
 // TODO: Check for failure. so it conforms to spec.
 // See: https://en.cppreference.com/w/cpp/io/basic_streambuf/overflow
  virtual int_type overflow(int_type character) override {
    bool eof = CharTraits::eq_int_type(
                        character, 
                        CharTraits::eof());
    if (!eof) {
      write(mFd, &character, 1);
      return CharTraits::not_eof(character);
    }
    return character;
  }

 private:
  int mFd;
};

static bool EnableLogging() {
  // Do not block. Will drop writes to pty if buffer is
  // full.
  int ptmxFD = open("/dev/ptmx", O_RDWR | O_NONBLOCK, 0);
  CHECK_ERR(ptmxFD, "Failed to open ptmx");
  ptsName = ptsname(ptmxFD);
  std::cout << "Slave PTY:" << ptsName << std::endl;
  grantpt(ptmxFD);
  unlockpt(ptmxFD);
  fileBuffer.reset(new FileBuf(ptmxFD));
  logoutPtr = std::make_unique<std::ostream>(fileBuffer.get());
  return true;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Must include either -s or -c to indicate server to client" << std::endl;
    return -1;
  }
  EnableLogging();
  term.Init();
  
  isClient = std::string(argv[1]) == "-c";
  if (isClient) {
    logout << "Operating client!" << std::endl;
    int clientSockFD = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in destSocketAddress;
    destSocketAddress.sin_family = AF_INET;
    destSocketAddress.sin_port = htons(PortNumber);
    destSocketAddress.sin_addr.s_addr = inet_addr(IPAddress); 
    int res = connect(clientSockFD, (sockaddr*)&destSocketAddress, sizeof(destSocketAddress));
    CHECK_ERR(res, "Failed to connect to server");
    sockFD = clientSockFD;
    MainLoop(clientSockFD);
    close(clientSockFD);
    return 0;
  }

  sockFD = socket(AF_INET, SOCK_STREAM, 0);
  CHECK_ERR(sockFD, "Failed to create Socket");

  struct sockaddr_in socketAddress; 
  socketAddress.sin_family = AF_INET;
  socketAddress.sin_port = htons(PortNumber);
  socketAddress.sin_addr.s_addr = inet_addr(IPAddress);
  int res = bind(sockFD, reinterpret_cast<sockaddr*>(&socketAddress), sizeof(sockaddr_in)); 
  CHECK_ERR(res, "Failed to bind to port 2500");
 
  res = listen(sockFD, 10);
  CHECK_ERR(res, "Failed to start listening");
  
  struct sockaddr_in clientAddressInfo;
  socklen_t length = sizeof(sockaddr_in);
  int connectionSocketFD = accept(sockFD, reinterpret_cast<sockaddr*>(&clientAddressInfo), &length);
  CHECK_ERR(connectionSocketFD, "Failed to accept incoming connection request");
  MainLoop(connectionSocketFD);
  close(connectionSocketFD);
  close(sockFD);
  return 0;
}
