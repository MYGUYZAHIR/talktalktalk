import Stockfish from "stockfish";

const engine = Stockfish();
engine.onmessage = (event) => {
  console.log("Stockfish:", event);
};

engine.postMessage("uci");
engine.postMessage("isready");

async function getAIMove(fen, depth = 12) {
  return new Promise((resolve) => {
    const listener = (event) => {
      const message = typeof event === "string" ? event : event.data;
      if (message.startsWith("bestmove")) {
        const move = message.split(" ")[1];
        engine.removeEventListener("message", listener);
        resolve(move);
      }
    };
    engine.addEventListener("message", listener);
    engine.postMessage(`position fen ${fen}`);
    engine.postMessage(`go depth ${depth}`);
  });
}
