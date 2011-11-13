package cc;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.HasTag;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.objectbank.TokenizerFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.process.WhitespaceTokenizer;
import edu.stanford.nlp.util.Function;

/**
 * based heavily on edu.stanford.nlp.process.DocumentPreprocessor
 */
public class SentenceTokeniser implements Iterable<List<HasWord>> {

  private Reader inputReader = null;

  //Configurable options
  private TokenizerFactory<? extends HasWord> tokenizerFactory = PTBTokenizer.factory();
  private String[] sentenceFinalPuncWords = {".", "?", "!"};
  private Function<List<HasWord>,List<HasWord>> escaper = null;
  private String sentenceDelimiter = null;

  //From PTB conventions
  private final String[] sentenceFinalFollowers = {")", "]", "\"", "\'", "''", "-RRB-", "-RSB-", "-RCB-"};

  public static void main(String[] args) throws IOException{          
    SentenceTokeniser sentenceTokeniser = new SentenceTokeniser();
    for (String sentence : sentenceTokeniser.extractSentences( "This is a sentence. And (do you know) So is this!")) {
      System.out.println("next sentence ["+sentence+"]");      
    }
    for (String sentence : sentenceTokeniser.extractSentences( "Oh, and by the way.... guess what! so is this!")) {
      System.out.println("next sentence ["+sentence+"]");      
    }
  }
  
  public List<String> extractSentences(String text) {
    this.inputReader = new StringReader(text);
    
    List<String> sentences = new ArrayList<String>();
    for (List<HasWord> sentence : this) {
      StringBuilder sentBuffer = new StringBuilder();
      for (HasWord word : sentence) {
        sentBuffer.append(word.word()+" ");
      }
      String sentenceWithoutTrailingSpace = sentBuffer.toString().substring(0, sentBuffer.length()-1); 
      sentences.add(sentenceWithoutTrailingSpace); 
    }
    return sentences;
  }
  
  /**
   * Returns sentences until the document is exhausted. Calls close() if the end of the document
   * is reached. Otherwise, the user is required to close the stream.
   */
  public Iterator<List<HasWord>> iterator() {
    return new PlainTextIterator();
  }

  private class PlainTextIterator implements Iterator<List<HasWord>> {

    private Tokenizer<? extends HasWord> tokenizer;
    private Set<String> sentDelims;
    private Set<String> delimFollowers = new HashSet<String>(Arrays.asList(sentenceFinalFollowers));
    private Function<String, String[]> splitTag;
    private List<HasWord> nextSent = null;
    private List<HasWord> nextSentCarryover = new ArrayList<HasWord>();

    public PlainTextIterator() {
      // Establish how to find sentence boundaries
      sentDelims = new HashSet<String>();
      boolean eolIsSignificant = false;
      if (sentenceDelimiter == null) {
        if (sentenceFinalPuncWords != null) {
          sentDelims = new HashSet<String>(Arrays.asList(sentenceFinalPuncWords));
        }
      } else {
        sentDelims.add(sentenceDelimiter);
        delimFollowers = new HashSet<String>();
        eolIsSignificant = sentenceDelimiter.matches("\\s+");
      }

      // Setup the tokenizer
      if(tokenizerFactory == null) {
        tokenizer = WhitespaceTokenizer.
          newWordWhitespaceTokenizer(inputReader, eolIsSignificant);
      } else {
        if(eolIsSignificant)
          tokenizerFactory.setOptions("tokenizeNLs");//wsg2010: This key currently used across all tokenizers
        tokenizer = tokenizerFactory.getTokenizer(inputReader);
      }

    }

    private void primeNext() {
      nextSent = new ArrayList<HasWord>(nextSentCarryover);
      nextSentCarryover.clear();
      boolean seenBoundary = false;

      while (tokenizer.hasNext()) {

        HasWord token = tokenizer.next();
        if (splitTag != null) {
          String[] toks = splitTag.apply(token.word());
          token.setWord(toks[0]);
          if(toks.length == 2 && token instanceof HasTag) {
            //wsg2011: Some of the underlying tokenizers return old
            //JavaNLP labels.  We could convert to CoreLabel here, but
            //we choose a conservative implementation....
            ((HasTag) token).setTag(toks[1]);
          }
        }

        if (sentDelims.contains(token.word())) {
          seenBoundary = true;
        } else if (seenBoundary && !delimFollowers.contains(token.word())) {
          nextSentCarryover.add(token);
          break;
        }

        if ( ! (token.word().matches("\\s+") //|| 
                /*token.word().equals(PTBLexer.NEWLINE_TOKEN)*/)) {
          nextSent.add(token);
        }

        // If there are no words that can follow a sentence delimiter,
        // then there are two cases.  In one case is we already have a
        // sentence, in which case there is no reason to look at the
        // next token, since that just causes buffering without any
        // chance of the current sentence being extended, since
        // delimFollowers = {}.  In the other case, we have an empty
        // sentence, which at this point means the sentence delimiter
        // was a whitespace token such as \n.  We might as well keep
        // going as if we had never seen anything.
        if (seenBoundary && delimFollowers.size() == 0) {
          if (nextSent.size() > 0) {
            break;
          } else {
            seenBoundary = false;
          }
        }
      }

      if (nextSent.size() == 0 && nextSentCarryover.size() == 0) {
        IOUtils.closeIgnoringExceptions(inputReader);
        inputReader = null;
        nextSent = null;
      } else if (escaper != null) {
        nextSent = escaper.apply(nextSent);
      }
    }

    public boolean hasNext() { 
      if (nextSent == null) {
        primeNext();
      }
      return nextSent != null; 
    }

    public List<HasWord> next() {
      if (nextSent == null) {
        primeNext();
      }
      if (nextSent == null) {
        throw new NoSuchElementException();
      }
      List<HasWord> thisIteration = nextSent;
      nextSent = null;
      return thisIteration;
    }

    public void remove() { throw new UnsupportedOperationException(); }
  }

}
