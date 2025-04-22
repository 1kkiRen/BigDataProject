from dataclasses import dataclass
from operator import sub, add
from typing import Tuple

from torch import Tensor


@dataclass
class Confusion:
	tp: int
	fp: int
	fn: int
	total: int

	@property
	def precision(self) -> float:
		p = self.tp + self.fp
		return self.tp / p if p > 0 else 0.0

	@property
	def recall(self) -> float:
		p = self.tp + self.fn
		return self.tp / p if p > 0 else 0.0

	@property
	def accuracy(self) -> float:
		return 1 - (self.fp + self.fn) / self.total


def confusion(
	logits: Tensor,
	target: Tensor,
) -> Confusion:
	pred = logits > 0

	tp = ((pred == 1) & (target == 1)).sum().item()
	fp = ((pred == 1) & (target == 0)).sum().item()
	fn = ((pred == 0) & (target == 1)).sum().item()
	total = pred.size(0)

	return Confusion(tp=tp, fp=fp, fn=fn, total=total)


class RollingMean:
	def __init__(self, n: int):
		self.n = n
		self.values = []
		self.sum = None

	def __call__(self, *values: float) -> Tuple[float, ...]:
		self.values.append(values)

		if self.sum is None:
			self.sum = values
		else:
			self.sum = tuple(map(add, self.sum, values))

		if len(self.values) > self.n:
			values = self.values.pop(0)
			self.sum = tuple(map(sub, self.sum, values))

		n = len(self.values)
		mean = tuple(s / n for s in self.sum)

		return mean
