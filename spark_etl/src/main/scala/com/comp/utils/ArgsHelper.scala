package com.comp.utils
import scala.annotation.tailrec

object ArgsHelper {

  final case class AddOptions(opt: String, longOpt: String, desc: String) {
    val symbol: Symbol = Symbol(opt)

    override def toString: String = "-%-5s --%-20s %s".format(opt, longOpt, desc)
  }

  type OptionMap = Map[Symbol, String]

  def createArgs(args: Array[String], optionsVec: Vector[AddOptions]): OptionMap = {
    val optMap = FlagsMap(optionsVec)
    val manPage = optMap.manPage
    if (args.isEmpty) throw new RuntimeException(s"Args error.\n$manPage")
    parseOption(Map(), args.toList, optMap, manPage)
  }

  @tailrec
  private def parseOption(options: OptionMap, args: List[String], flagsMap: FlagsMap, manPage: String): OptionMap =
    args match {
      case Nil => options
      case key :: value :: tail if key.head == '-' && flagsMap.matchKey(key) =>
        parseOption(options ++ Map(flagsMap.getFlag(key) -> value), tail, flagsMap, manPage)
      case option :: tail => throw new RuntimeException(s"Unknown option '$option'\n$manPage")
    }

  final private case class FlagsMap(private val optionsVec: Vector[AddOptions]) {
    val optSymMap: Map[AddOptions, Symbol] = optionsVec.map(opt => (opt, opt.symbol)).toMap
    val manPage: String = optSymMap.keys.toList.sortBy(_.opt).mkString("\n")

    private val keys = optSymMap.keys

    def matchKey(searchKey: String): Boolean = keys.exists(findMatch(_, searchKey))

    def getFlag(searchKey: String): Symbol = {
      val foundKey = keys.filter(findMatch(_, searchKey)).head
      optSymMap(foundKey)
    }

    private def findMatch(key: AddOptions, searchKey: String): Boolean = {
      val cleansedKey = searchKey.replaceFirst("(--|-)", "")
      key.opt == cleansedKey || key.longOpt == cleansedKey
    }
  }
}
